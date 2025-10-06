#define DUCKDB_EXTENSION_MAIN

#include "dns_query_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// DNS resolution includes
#include <vector>
#include <string>
#include <cstring>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <future>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <resolv.h>
#include <arpa/nameser.h>

// DNS constants
#ifndef C_IN
#define C_IN 1
#endif
#ifndef T_CNAME
#define T_CNAME 5
#endif

namespace duckdb {

// Global DNS cache with thread safety
static std::unordered_map<std::string, std::string> dns_cache;
static std::mutex cache_mutex;

std::string dns_cname_lookup_uncached(const std::string& hostname) {
    unsigned char answer[4096];
    int len = res_query(hostname.c_str(), C_IN, T_CNAME, answer, sizeof(answer));
    if (len <= 0)
        return {};

    ns_msg handle;
    if (ns_initparse(answer, len, &handle) != 0)
        return {};

    int count = ns_msg_count(handle, ns_s_an);
    for (int i = 0; i < count; i++) {
        ns_rr rr;
        if (ns_parserr(&handle, ns_s_an, i, &rr) == 0 && ns_rr_type(rr) == T_CNAME) {
            char cname[1024];
            int cname_len = dn_expand(ns_msg_base(handle), ns_msg_end(handle),
                                      ns_rr_rdata(rr), cname, sizeof(cname));
            if (cname_len >= 0)
                return std::string(cname);
        }
    }

    return {};
}

std::string dns_cname_lookup(const std::string& hostname) {
    // Check cache first
    {
        std::lock_guard<std::mutex> lock(cache_mutex);
        auto it = dns_cache.find(hostname);
        if (it != dns_cache.end()) {
            return it->second;
        }
    }

    // Cache miss - perform actual lookup
    std::string result = dns_cname_lookup_uncached(hostname);

    // Store in cache
    {
        std::lock_guard<std::mutex> lock(cache_mutex);
        dns_cache[hostname] = result;
    }

    return result;
}

inline void DnsCnameLookupScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &hostname_vector = args.data[0];
	auto count = args.size();

	// Collect unique hostnames that need DNS lookups
	std::vector<std::string> unique_hostnames;
	std::unordered_map<std::string, idx_t> hostname_to_index;

	{
		std::lock_guard<std::mutex> lock(cache_mutex);
		for (idx_t i = 0; i < count; i++) {
			auto hostname = hostname_vector.GetValue(i).ToString();
			if (dns_cache.find(hostname) == dns_cache.end() &&
				hostname_to_index.find(hostname) == hostname_to_index.end()) {
				hostname_to_index[hostname] = unique_hostnames.size();
				unique_hostnames.push_back(hostname);
			}
		}
	}

	// Perform parallel DNS lookups for uncached hostnames
	std::vector<std::future<std::string>> futures;
	const size_t max_threads = std::min((size_t)8, unique_hostnames.size());

	for (size_t i = 0; i < unique_hostnames.size(); i += max_threads) {
		futures.clear();

		// Start up to max_threads parallel lookups
		for (size_t j = 0; j < max_threads && (i + j) < unique_hostnames.size(); j++) {
			const auto& hostname = unique_hostnames[i + j];
			futures.push_back(std::async(std::launch::async, dns_cname_lookup_uncached, hostname));
		}

		// Collect results and update cache
		{
			std::lock_guard<std::mutex> lock(cache_mutex);
			for (size_t j = 0; j < futures.size(); j++) {
				const auto& hostname = unique_hostnames[i + j];
				dns_cache[hostname] = futures[j].get();
			}
		}
	}

	// Now process all results using cached values
	UnaryExecutor::Execute<string_t, string_t>(
		hostname_vector, result, count,
		[&](string_t hostname) {
			std::lock_guard<std::mutex> lock(cache_mutex);
			auto it = dns_cache.find(hostname.GetString());
			return StringVector::AddString(result, it != dns_cache.end() ? it->second : "");
		});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register dns_cname_lookup function
	auto dns_cname_lookup_function = ScalarFunction("dns_cname_lookup",
		{LogicalType::VARCHAR},
		LogicalType::VARCHAR,
		DnsCnameLookupScalarFun);
	loader.RegisterFunction(dns_cname_lookup_function);
}

void DnsQueryExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string DnsQueryExtension::Name() {
	return "dns_query";
}

std::string DnsQueryExtension::Version() const {
#ifdef EXT_VERSION_DNS_QUERY
	return EXT_VERSION_DNS_QUERY;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(dns_query, loader) {
	duckdb::LoadInternal(loader);
}
}

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


std::string dns_cname_lookup(const std::string& hostname) {
    res_init();  // ensure resolver is initialized

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

inline void DnsCnameLookupScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &hostname_vector = args.data[0];

	UnaryExecutor::Execute<string_t, string_t>(
		hostname_vector, result, args.size(),
		[&](string_t hostname) {
			auto cname_result = dns_cname_lookup(hostname.GetString());
			return StringVector::AddString(result, cname_result);
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

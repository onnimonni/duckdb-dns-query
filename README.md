# Archived. Use https://github.com/tobilg/duckdb-dns instead of this

# DuckDB DNS CNAME extension

**THIS IS VERY EXPERIMENTAL AND CREATED WITH AI ASSISTANCE**

This is a proof of concept duckdb extension for DNS queries:
```
SELECT dns_cname_lookup('fligts.google.com');
```

On MacOS it takes 17 minutes to query 5k domains so I don't recommend to use this.

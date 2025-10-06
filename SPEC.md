Add following functionality:
```sql
SELECT dns_lookup('flights.google.com','CNAME') as cname;
┌───────────────────────┐
│         cname         │
│       varchar[]       │
├───────────────────────┤
│ [www3.l.google.com.]  │
└───────────────────────┘
```

**IMPORTANT: It needs to be a scalar function instead of table function.**

**IMPORTANT: Use c-ares.org library through vckpg for the dns lookups**
TSHISTORY SUPERVISION
======================

# Purpose

This component provides a supervision mechanism over
[tshistory][tshistory].

The central use case is as follows:

* time series from third party providers are stored as is

* from time to time, a data analyst will want to override (bogus)
  values, and need to know what values were overriden

* when upstream fix its bogus values the fixed values must override
  the data analyst fixes

* the complete series history must retain information on the
  supervision activity

[tshistory]: https://bitbucket.org/pythonian/tshistory

# Usage

The basic [tshistory][tshistory] usage is described on its own
documentation.

An override is made as follows:

```python
 >>> tsh.insert(engine, series, 'my_series', 'analyst@corp.com', manual=True)
```

Inserted values will show up in the next `.get` call.

Specific API calls exist to provide a standard workflow:

* `.get_overrides` returns a series of all the manual (and still
  *current*) insertions

* `.get_ts_marker` returns a couple *series* and *markers* where
  *series* is like the output of a standard `.get` call and *markers*
  a boolean series indicating whether any data point is from
  *upstream* or a manual edition.


# time bounds

To set time bounds for a variable, it needs to meet the following conditions:

- time method can be mean or instantanous or climatology. time bounds is a valid option when time methods is either mean or instantanous.
- in case of instantaneous time method, time bounds are same as the time values. That is delta time is 0 for the bounds for a given time point.
- in case of mean time method, delta time of the bounds is greater than 0. the time point can be at the start or middle or at the end of the time interval (bounds).
- setting time bounds before doing time average has the advantage of setting the bounds more accurately. `approx_interval` aids in determining time bounds.
- setting time bounds after doing time average is possible but the time bounds many not be accurate if time points are set to middle or end.

say time bounds function has the following signature:

```python
def time_bounds(ds: xr.Dataset, rule: Rule) -> xr.Dataset:
    pass
```

`approx_interval` which is read from CMIP Table is accessible from rule like `rule.approx_interval`.
`approx_interval` is floating number in days.

if approx_interval is same as data frequency, it can either mean that the data is already time averaged or the data had similar frequency as the approx_interval.

Let's say data is at monthly frequency and approx_interval is 30 days. As both data frequency andn approx_interval are the same, if the data points are at start of the month, time bounds can be set as the (this_month_start, next_month_start) If the points are at middle or at end of the month, time bounds are still (this_month_start, next_month_start)

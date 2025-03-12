# Flink starter

## Gotchas

### Not having `set_parallelism(1)` set when trying to implement a window assigner

I was able to get `pyflink` working pretty well with reducefunctions. However I ran into an issue when trying to implement a sliding window assigner. The bug in my code was the following line missing:

```python
env.set_parallelism(1)
```

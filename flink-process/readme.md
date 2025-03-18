# Flink Table

## Getting set up

```bash
`confluent local kafka start`
```

Once this command has run, the broker port will be printed in the console. The plaintext broker port will be used in `flink_table.py`.

## Finishing off

Stop kafka:

```bash
confluent local kafka stop
```

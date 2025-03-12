# Processing Github Firehose with PyFlink

## Aknowledegments

The work in this repository would not have been possible if not for [augustodn](https://github.com/augustodn/pyflink-iot-alerts). His repository (particularly in getting set up with `pyenv` and the `requirements.txt` file) were absolute lifesavers. [Jaehyeon Kim's blog](http://jaehyeon.me/blog/2023-08-17-getting-started-with-pyflink-on-aws-part-1/#source-data) was also invaluable in getting unstuck.

## Set up

This repository should equip you to be able to process the stream from the [Github firehose](https://github-firehose.libraries.io/). The following readme will give instructions that should lead to the top 5 committers in Github of the past 10 seconds to be printed in your terminal.

## Start ingesting the Github firehose using Kafka and Quix streams

Navigate to the `kafka-process` directory. Note that you will need to have [Quix streams](https://quix.io/get-started-with-quix-streams) and Kafka installed for this to work.

```bash
cd kafka-process

sudo quix pipeline up
```

We will want to create a new virtual environment in the `kafka-process` folder and install the requisite dependencies:

```bash
python3 -m venv .venv

source .venv/bin.activate

pip install -r requirements.txt
```

Finally, run the `main.py` script to get Kafka up-and-running on your machine.

```bash
python3 main.py
```

You should start to see events appearing in the terminal, indicating that the python script is running and you are receiving Github Firehose events.

## Start the Flink Processing

Run and install the pyenv installation script

```bash
curl https://pyenv.run | bash
```

Setup `pyenv` in the shell:

```
echo -e 'export PYENV_ROOT="$HOME/.pyenv"\nexport PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo -e 'eval "$(pyenv init --path)"\neval "$(pyenv init -)"' >> ~/.bashrc
source ~/.bashrc
```

Then install and load the desired python version:

```bash
pyenv install 3.10
pyenv local 3.10
```

Install package requirements:

```bash
pip install -r requirements.txt
```

Download the Flink Kafka Connect JAR:

```bash
wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar
```

and move this into the `flink-process` folder:

```bash
mv ~Downloads/flink-sql-connector-kafka-3.1.0-1.18.jar ./flink-process/
```

Open a new terminal tab and navigate to the `kafka-process` folder.

Once again, fire up a virtual environment and install the requirements:

```bash
python3 -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt
```

Run the Kafka consumer script:

```bash
python3 kafa_consumer.py
```

# SBT Quickstart 

## Environment Versions

| Component    | Docker/SBT                    | Colab/PySpark |
| ------------ | ----------------------------- | ------------- |
| Scala        | 2.13.16                       | N/A           |
| SBT          | 1.11.7                        | N/A           |
| Spark        | 4.0.0                         | 3.5.1         |
| Delta Lake   | 4.0.0                         | 3.2.0         |
| Docker image | `deltaio/delta-docker:latest` | N/A           |

## Run in Docker (Scala + SBT)

### 1. Start the Delta Docker container

```bash
docker run --name delta-lake-lab --rm -it -p 4040:4040 -u root --entrypoint bash deltaio/delta-docker
```

* `--rm` → automatically remove the container when stopped
* `-p 4040:4040` → exposes Spark UI on port 4040
* `-u root` → run as root to install packages
* `--entrypoint bash` → opens a bash shell

### 2. Install SBT and utilities

Inside the container:

```bash
apt-get update
apt-get install -y apt-transport-https curl jq

# Add SBT repository key
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add -

# Add SBT repository
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list

# Update and install SBT
apt-get update
apt-get install -y sbt
```

### 3. Clone the lab repository

```bash
git clone https://github.com/CynicDog/delta-lake-lab.git
cd delta-lake-lab/delta-lake-lab
```

### 4. Project structure

```
delta-lake-lab/
├── build.sbt           # SBT build configuration
├── README.md           # This file
├── src/                # Scala source files
│   └── main/scala/
│       ├── DeltaApp.scala
│       └── 01_table_batch_reads_and_writes/
│           └── DeltaMetaExample.scala
└── target/             # SBT build outputs (ignored in Git)
```

### 5. Run the project

```bash
sbt run
```

* SBT will compile the project and prompt you to select the main class.
* Enter the number corresponding to the app you wish to run.

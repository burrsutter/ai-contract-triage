rm -rf ./data/intake/done

/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --delete --topic documents
/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --create --topic documents --partitions 1 --replication-factor 1


/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --delete --topic contracts
/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --create --topic contracts --partitions 1 --replication-factor 1

/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --delete --topic structured-contracts
/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --create --topic structured-contracts --partitions 1 --replication-factor 1

/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --delete --topic reports
/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --create --topic reports --partitions 1 --replication-factor 1

/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --delete --topic structured-reports
/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --create --topic structured-reports --partitions 1 --replication-factor 1

/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --delete --topic patients
/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --create --topic patients --partitions 1 --replication-factor 1

/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --delete --topic structured-patients
/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --create --topic structured-patients --partitions 1 --replication-factor 1

/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --delete --topic document-review
/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --create --topic document-review --partitions 1 --replication-factor 1

/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --delete --topic invoices
/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --create --topic invoices --partitions 1 --replication-factor 1

/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --delete --topic structured-invoices
/opt/homebrew/bin/kafka-topics --bootstrap-server localhost:9092 --create --topic structured-invoices --partitions 1 --replication-factor 1

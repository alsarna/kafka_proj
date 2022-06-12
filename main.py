from pyflink.common import JsonRowDeserializationSchema, Types, WatermarkStrategy, Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.table import StreamTableEnvironment
from pyflink.datastream.connectors import FileSink, OutputFileConfig
from pyflink.common.serialization import Encoder


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    env.add_jars("file:/home/students/s424343/PycharmProjects/kafka_proj/flink-sql-connector-kafka-1.15.0.jar")

    type_info = Types.ROW_NAMED(["Bitcoin", "eth", "ltc", "usd", "aud", "cad", "chf", "eur", "gbp", "pln"],
                                [Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(),
                                 Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()])
    deserialization_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()

    kafkaSource = FlinkKafkaConsumer(
        topics='bitcoin',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': '150.254.78.69:29092',
                    'group.id': 's424343'}
    )
    kafkaSource.set_start_from_earliest()

    ds = env.add_source(kafkaSource).assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(20)))
    ds.print()

    # convert a DataStream to a Table
    table = t_env.from_data_stream(ds)

    print('\ntable data')
    print(table.print_schema())

    output_path = './output/'
    file_sink = FileSink \
        .for_row_format(output_path, Encoder.simple_string_encoder()) \
        .with_output_file_config(OutputFileConfig.builder().with_part_prefix('pre').with_part_suffix('suf').build()) \
        .build()

    ds.sink_to(file_sink)

    env.execute()

if __name__ == '__main__':
    main()
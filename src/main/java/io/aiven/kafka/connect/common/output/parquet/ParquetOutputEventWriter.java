/*
 * Copyright 2021 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.common.output.parquet;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.output.Event;
import io.aiven.kafka.connect.common.output.OutputStreamWriter;
import io.aiven.kafka.connect.common.output.OutputWriter;
import io.aiven.kafka.connect.common.output.SinkRecordConverter;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import org.apache.avro.reflect.ReflectData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ParquetOutputEventWriter extends OutputWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetOutputEventWriter.class);

    private static final int PAGE_SIZE = 64 * 1024;

    private final SinkRecordConverter sinkRecordConverter;

    private final ParquetSchemaBuilder parquetSchemaBuilder;

    public ParquetOutputEventWriter(final Collection<OutputField> fields,
                                    final OutputStream out,
                                    final Map<String, String> externalConfig,
                                    final boolean envelopeEnabled) {
        super(new ParquetPositionOutputStream(out), new OutputStreamWriterStub(), externalConfig);
        final var avroData = new AvroData(new AvroDataConfig(externalConfig));
        this.sinkRecordConverter = new SinkRecordConverter(fields, avroData, envelopeEnabled);
        this.parquetSchemaBuilder = new ParquetSchemaBuilder(fields, avroData, envelopeEnabled);
    }

    @Override
    public void writeRecords(final Collection<SinkRecord> sinkRecords) throws IOException {
        final var parquetConfig = new ParquetConfig(externalConfiguration);
        final var parquetSchema = ReflectData.get().getSchema(Event.class);
        LOGGER.debug("Record schema is: {}", parquetSchema);

        try (final var parquetWriter =
                     AvroParquetWriter.builder(new ParquetOutputFile())
                             .withSchema(parquetSchema)
                             .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                             .withDictionaryEncoding(true)
                             .withDataModel(ReflectData.get())
                             .withConf(parquetConfig.parquetConfiguration())
                             .withCompressionCodec(CompressionCodecName.SNAPPY)
                             .withCompressionCodec(parquetConfig.compressionCodecName())
                             .withPageSize(PAGE_SIZE)
                             .build()) {
            for (final var record : sinkRecords) {
                parquetWriter.write(new Event(record.value().toString()));
            }
        } catch (final JSONException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeRecord(final SinkRecord record) throws IOException {
        this.writeRecords(List.of(record));
    }

    private static final class OutputStreamWriterStub implements OutputStreamWriter {
        @Override
        public void writeOneRecord(final OutputStream outputStream, final SinkRecord record) throws IOException {
        }
    }

    private class ParquetOutputFile implements OutputFile {

        @Override
        public PositionOutputStream create(final long blockSizeHint) throws IOException {
            return (ParquetPositionOutputStream) outputStream;
        }

        @Override
        public PositionOutputStream createOrOverwrite(final long blockSizeHint) throws IOException {
            return create(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }

}

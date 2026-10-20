# Changelog

All notable changes to this project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.8.0]

### Breaking changes
- The Maven `groupId` changed from `de.unknownreality` to `dev.gruen`, and the Java packages were renamed from `de.unknownreality.dataframe` to `dev.gruen.dataframe`.
- Java 21 or newer is required (`maven.compiler.release=21`). Before, Java 8 was the baseline.
- UTF-8 is the default charset of `DataReader` and `DataWriter`. Before, the platform charset was used.
  Use `withCharset(...)` on `CSVReaderBuilder` / `CSVWriterBuilder` to read or write other encodings.
- Removed deprecated `DataFrameBuilder.createFrom(DataIterator)` and `CSVIterator.toDataFrame()`.
  Use `DataFrame.load(...)`, `DataFrame.fromCSV(...)` or `DataFrame.builder().from(iterator)`.

### Added
- `DataFrame.print(PrintStream)` and `DataFrame.print(Appendable)` plus matching `DataFrameWriter.print(...)` overloads.
- `BufferedStreamIterator(InputStream, Charset)` constructor. `BufferedStreamIterator` (and `CSVIterator`) are `AutoCloseable`.
- Gzip compressed input is detected on every binary source (file, stream, url, resource, bytes).

### Fixed
- Streams and readers were not closed if creating the iterator failed (`DataReader`, `CSVIterator`, `DataFrameMetaReader`).
- gzip files were read and written with the platform charset, `CSVWriter` ignored the writer charset for gzip output.
- `CSVWriter.write(OutputStream)` with gzip did not finish the gzip stream.
- `StringType.read(ByteBuffer)` did not read the string bytes, `StringType.read(DataInputStream)` used `read` instead of `readFully`.

## [0.7.5]
- Direct value access for `DataRow` objects. `DataRow` objects are invalidated once the source `DataFrame` changes.
- Row collections are returned as `DataRows` objects, which can be converted to a new `DataFrame`.
- Improved `groupBy`.

## [0.7.0]
- The read and write functions were rewritten. Some existing methods were removed.
- Data grouping was refactored. Aggregation functions can be applied to data groupings.
- Java 8 is required.
- Empty `DataFrame` instances are created using `DataFrame.create()`.

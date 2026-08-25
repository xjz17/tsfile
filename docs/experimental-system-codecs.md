# Experimental BOS and Sub-column encodings

This branch adds two lossless numeric encodings to the Java TsFile read/write path:

- `BOS` (`TSEncoding` id 15): per-page blocking, delta transform, adaptive outlier separation, frame-of-reference residuals, and bit packing.
- `SUBCOLUMN` (`TSEncoding` id 16): per-page blocking, frame-of-reference residuals, 4-bit sub-columns, and per-group selection among bit packing, RLE, and dictionary coding.

Both encodings support `INT32`, `DATE`, `INT64`, `TIMESTAMP`, `FLOAT`, and `DOUBLE`. Floating-point pages first try exact decimal scaling and otherwise store raw IEEE bits. Decoding is bit-exact for NaNs, infinities, and negative zero.

`REGER` is not added to `TSEncoding`: REGER jointly encodes timestamps and multiple value columns, while TsFile's production `Encoder`/`Decoder` contract is a single-column page interface. Assigning a REGER id there would misrepresent the algorithm and could not be decoded transparently by existing readers.

## Verification and benchmark

The unit test `ExperimentalLongCodecTest` covers randomized/extreme payloads, corrupt input, factory construction, all supported primitive types, and a physical TsFile write/read round trip.

`org.apache.tsfile.SystemCodecBenchmark` reads the shared `SCDBIN01` interchange file and measures physical file write and full query read wall time for BOS, Sub-column, and the compatible built-in double encodings. The benchmark uses `UNCOMPRESSED` as the outer compressor and checks every decoded IEEE bit before reporting `hash_ok=true`. Sprintz is excluded because the v2.4.0 implementation failed this bit-exact double round-trip check.

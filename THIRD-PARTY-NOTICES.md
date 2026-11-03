# Third-party notices

Each component below keeps its own license. This repository's `LICENSE` (MIT) covers only the code written for it, mainly `native/` and `bench/`.

## ABIDES

`abides/` holds ABIDES at commit `f9cbe51` with the four official Agenthon Track 3 patches and Python-level speedups. ABIDES is BSD-3-Clause, © 2019 Georgia Tech Research Corporation; the full text is in [abides/LICENSE-ABIDES.txt](abides/LICENSE-ABIDES.txt).

## Agenthon 2026 Track 3 public kit

The `abides_fork/` adapter, the Track 3 patches, and the scenarios and task cards under `native/pgo/` come from [Agenthon-2026/track3-simulation-public](https://github.com/Agenthon-2026/track3-simulation-public), released under this license:

```text
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## Rust crates

`native/` depends on `serde_json`, `sha2`, `parquet` and `bytes`, each under its own license (MIT or Apache-2.0). They are fetched by Cargo, not copied into this repository.

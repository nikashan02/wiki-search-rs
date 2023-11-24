# wiki-search-rs

A search engine for Wikipedia written entirely in Rust.

## Features

- Full text search
- [Okapi BM25](https://en.wikipedia.org/wiki/Okapi_BM25) ranking model
- Article snippets in results
- Stream XML proccessing
- Concurrent indexing

Coming soon:

- Advanced query language support
- Multi-language support
- Other ranking models (vector-space, etc.)

## Prerequisites

To build your own Wikiepedia index to run the search engine, you'll need a copy of the latest Wikipedia dump XML in `.bz2` compressed format. This can be obtained [here](https://dumps.wikimedia.org/enwiki/latest/). You'll want `pages-articles-multistream.xml.bz2` file. Keep in mind, the file is over 20GB compressed and expands to over 100GB uncompressed when the index is built.

## Usage

To build the index:

```
cargo run -- --build-index --wiki-dump-path <path-to-wikipedia-dump> --index-path <path-to-output-index>
```

This will save the articles, lexicon, and inverted index to your disk. To run a search query:

```
cargo run -- --index-path <path-to-built-index> --search "<search-query>" --num-max-results <optional-limit-num-results>
```

## Building

Building the release version is as simple as running the following in a terminal:

```
cargo build --release
```

The executeable can then be found in `target/release`.

# hsfive — HDF5 in pure Haskell

## Goal

The goal of this project is to implement at least a read-only version of an HDF5 library, ideally compatible with the filter plugins, so you can read, for example, Dectris detector data.

## Why on earth would you do this?

1. Fun.
2. If we implement all of (the very common things in) HDF5, including plugins, then we can have Haskell-only, statically linked, portable HDF5 tools, and isn't that super cool?

## Current state

This is very much a piece of nothing right now, don't take the project too seriously. Once it's got Haddocks and is on hackage, take notice of this.

We're trying to implement "support" for all sample HDF5 files on the [silx.org](http://www.silx.org/pub/h5web/) website. Currently we can actually parse (and thus partially h5dump) these:

- `water_224.h5`
- `epics.h5`
- `dtype.h5`
- `dtype_*.h5`: can be read, but it only contains attributes confusing our own H5Dump tool
- `destfile.h5`
- `compressed-virtual.h5`
- `compressed.h5`
- `complex_aux.h5`
- `committed_type.h5`
- `braggy.h5`
- `bamboo.h5`
- `201805_WSe2_arpes.nxs`
- `esrf_id15_example1.h5`
- `ewoksid31.h5`
- `file_fail.nxs`
- `file_ok.nxs`
- `filters.h5`
- `filters_jpeg.h5`

**Not** supported are:

- `nxnote.h5`, because this has hardlinks in it to nonexistant files

### C bits

LZ4/Bitshuffle is currently still C code, look at the `cbits` directory. At some point I'd like to change this, but compiling a few C files is a good tradeoff to separately compiling, shipping and linking the bitshuffle plugin DLL.

# PKGStream

Generic HTTP streaming server for files contained in local archives.

## Current MVP

- recursively list archive entries;
- expose a selected entry through HTTP;
- stream without creating an extracted copy on disk;
- keep the transfer bounded by Node HTTP backpressure;
- implement single-range HTTP requests (206 Partial Content);
- support RAR/ZIP/7z through an installed 7zz, 7z, or unrar executable;
- restrict archive paths to a configured root directory.

For compressed entries, a non-zero HTTP range currently starts the decoder from the beginning and discards bytes until the requested offset. This is correct but not yet optimized for random access.

## Architecture

```
local archive
     |
     v
archive backend (7zz / 7z / unrar)
     |
     +---- list entries
     |
     +---- decode selected entry
                  |
                  v
             HTTP server
                  |
             GET + Range
```

The archive is never fully extracted by PKGStream. Decoded bytes flow through the process pipe directly into the HTTP response.

## Setup

Node.js 20+ is recommended.

Install an archive extractor available on your platform:

- `7zz` or `7z` for RAR/ZIP/7z
- `unrar` for RAR

Then:

```bash
npm install
mkdir -p archives
npm test
PKGSTREAM_ROOT=./archives npm start
```

Put a test archive you are authorized to use inside `archives/`.

## API

### List

```
GET /list?archive=test.rar
```

Returns archive entries and their sizes when the backend provides them.

### Stream

```
GET /stream?archive=test.rar&entry=folder/file.bin
```

The response supports Content-Length, Accept-Ranges, Range, 206 Partial Content, and 416 Range Not Satisfiable.

### Health

```
GET /health
```

## Security model

The server does not accept arbitrary filesystem paths. archive= is resolved underneath PKGSTREAM_ROOT.

The project has no content catalog, search service, or third-party content source. It is a generic transport/extraction component for files the operator is authorized to access and transmit.

## Roadmap

1. RAR/ZIP/7z backend abstraction — done
2. HTTP streaming — done
3. HTTP Range — done
4. Recursive entry selection — done
5. Multipart RAR sets
6. Remote HTTP archive source with ranged reads
7. Native incremental RAR backend, removing the external extractor requirement
8. Efficient random-access Range support
9. Android UI / local network discovery

## Development

```bash
npm test
npm start
```


## Remote RAR backend

PKGStream can now read authorized RAR3/RAR4/RAR5 sources directly over HTTP/HTTPS using ranged reads. The backend uses `@mary/rar`, whose `fromFetch` reader is designed for ranged HTTP fetches and whose entries expose a streaming body. citeturn4view0

List a remote archive with `GET /list?source=<URL>`. Stream a selected entry with `GET /stream?source=<URL>&entry=<path>`. Multipart sets can be supplied by repeating `source=` in volume order. Downstream HTTP Range is supported; a requested output range may still require decoding from the beginning of the selected entry.

Install with `npm install`; the checked-in `.npmrc` configures JSR's npm compatibility registry for Node/npm. citeturn3search0turn3search3

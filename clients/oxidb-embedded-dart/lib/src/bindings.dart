import 'dart:ffi';
import 'dart:io';

import 'package:ffi/ffi.dart';

// C surface of liboxidb_embedded_ffi (see oxidb_embedded.h):
//   OxiDbHandle* oxidb_open(const char* path);
//   OxiDbHandle* oxidb_open_encrypted(const char* path, const char* key_path);
//   OxiDbHandle* oxidb_open_encrypted_bytes(const char* path, const unsigned char* key, size_t len);
//   void         oxidb_close(OxiDbHandle*);
//   char*        oxidb_execute(OxiDbHandle*, const char* cmd_json);
//   void         oxidb_free_string(char*);

typedef _OpenC = Pointer<Void> Function(Pointer<Utf8>);
typedef _OpenDart = Pointer<Void> Function(Pointer<Utf8>);
typedef _OpenEncC = Pointer<Void> Function(Pointer<Utf8>, Pointer<Utf8>);
typedef _OpenEncDart = Pointer<Void> Function(Pointer<Utf8>, Pointer<Utf8>);
typedef _OpenEncBytesC = Pointer<Void> Function(Pointer<Utf8>, Pointer<Uint8>, UintPtr);
typedef _OpenEncBytesDart = Pointer<Void> Function(Pointer<Utf8>, Pointer<Uint8>, int);
typedef _CloseC = Void Function(Pointer<Void>);
typedef _CloseDart = void Function(Pointer<Void>);
typedef _ExecC = Pointer<Utf8> Function(Pointer<Void>, Pointer<Utf8>);
typedef _ExecDart = Pointer<Utf8> Function(Pointer<Void>, Pointer<Utf8>);
typedef _FreeC = Void Function(Pointer<Utf8>);
typedef _FreeDart = void Function(Pointer<Utf8>);

/// Resolved native bindings. Loaded once per process.
class Bindings {
  Bindings._(DynamicLibrary lib)
      : open = lib.lookupFunction<_OpenC, _OpenDart>('oxidb_open'),
        openEncrypted =
            lib.lookupFunction<_OpenEncC, _OpenEncDart>('oxidb_open_encrypted'),
        openEncryptedBytes = lib.lookupFunction<_OpenEncBytesC, _OpenEncBytesDart>(
            'oxidb_open_encrypted_bytes'),
        close = lib.lookupFunction<_CloseC, _CloseDart>('oxidb_close'),
        execute = lib.lookupFunction<_ExecC, _ExecDart>('oxidb_execute'),
        freeString = lib.lookupFunction<_FreeC, _FreeDart>('oxidb_free_string');

  final _OpenDart open;
  final _OpenEncDart openEncrypted;
  final _OpenEncBytesDart openEncryptedBytes;
  final _CloseDart close;
  final _ExecDart execute;
  final _FreeDart freeString;

  static Bindings? _instance;

  /// Override the library location BEFORE the first [OxiDb.open] — used by
  /// host-side tests and by apps that bundle the library under a
  /// non-default name. On Android the `.so` ships in the APK's jniLibs; on
  /// iOS/macOS the static library is linked into the app binary, so the
  /// symbols are found in the process itself.
  static String? libraryPath;

  static Bindings instance() {
    return _instance ??= Bindings._(_load());
  }

  static DynamicLibrary _load() {
    final override = libraryPath;
    if (override != null) {
      return DynamicLibrary.open(override);
    }
    if (Platform.isAndroid) {
      return DynamicLibrary.open('liboxidb_embedded_ffi.so');
    }
    if (Platform.isIOS) {
      // Statically linked into the app binary (xcframework).
      return DynamicLibrary.process();
    }
    if (Platform.isMacOS) {
      // A Flutter macOS app links the static library into the binary; a
      // plain Dart process loads the dylib by name (or via [libraryPath]).
      try {
        return DynamicLibrary.open('liboxidb_embedded_ffi.dylib');
      } catch (_) {
        return DynamicLibrary.process();
      }
    }
    if (Platform.isLinux) {
      return DynamicLibrary.open('liboxidb_embedded_ffi.so');
    }
    if (Platform.isWindows) {
      return DynamicLibrary.open('oxidb_embedded_ffi.dll');
    }
    throw UnsupportedError('unsupported platform: ${Platform.operatingSystem}');
  }
}

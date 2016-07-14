require 'ffi'

module NativeFile
  extend FFI::Library
  ffi_lib 'c'
  attach_function :fopen, [ :string, :string ], :pointer
  attach_function :fclose, [ :pointer], :int
  attach_function :fwrite, [:pointer, :int, :int, :pointer], :int
  attach_function :fread, [:pointer, :int, :int, :pointer], :int
end
require 'ffi'

module NativeMalloc
  extend FFI::Library
  ffi_lib 'c'
  attach_function :malloc, [ :int ], :pointer
end
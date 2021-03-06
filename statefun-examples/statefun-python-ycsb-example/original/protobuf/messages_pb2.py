# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: messages.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import any_pb2 as google_dot_protobuf_dot_any__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='messages.proto',
  package='yscb_example.messages',
  syntax='proto3',
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\x0emessages.proto\x12\x15yscb_example.messages\x1a\x19google/protobuf/any.proto\"D\n\x07Wrapper\x12\x12\n\nrequest_id\x18\x01 \x01(\t\x12%\n\x07message\x18\x02 \x01(\x0b\x32\x14.google.protobuf.Any\"\x81\x01\n\x05State\x12\x38\n\x06\x66ields\x18\x01 \x03(\x0b\x32(.yscb_example.messages.State.FieldsEntry\x12\x0f\n\x07\x62\x61lance\x18\x02 \x01(\x05\x1a-\n\x0b\x46ieldsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"A\n\x06Insert\x12\n\n\x02id\x18\x01 \x01(\t\x12+\n\x05state\x18\x02 \x01(\x0b\x32\x1c.yscb_example.messages.State\"\x12\n\x04Read\x12\n\n\x02id\x18\x01 \x01(\t\"\x81\x01\n\x06Update\x12\n\n\x02id\x18\x01 \x01(\t\x12;\n\x07updates\x18\x02 \x03(\x0b\x32*.yscb_example.messages.Update.UpdatesEntry\x1a.\n\x0cUpdatesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"Z\n\x08Response\x12\x12\n\nrequest_id\x18\x01 \x01(\t\x12\x13\n\x0bstatus_code\x18\x02 \x01(\x05\x12%\n\x07message\x18\x03 \x01(\x0b\x32\x14.google.protobuf.Anyb\x06proto3'
  ,
  dependencies=[google_dot_protobuf_dot_any__pb2.DESCRIPTOR,])




_WRAPPER = _descriptor.Descriptor(
  name='Wrapper',
  full_name='yscb_example.messages.Wrapper',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='request_id', full_name='yscb_example.messages.Wrapper.request_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='message', full_name='yscb_example.messages.Wrapper.message', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=68,
  serialized_end=136,
)


_STATE_FIELDSENTRY = _descriptor.Descriptor(
  name='FieldsEntry',
  full_name='yscb_example.messages.State.FieldsEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='yscb_example.messages.State.FieldsEntry.key', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='value', full_name='yscb_example.messages.State.FieldsEntry.value', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=b'8\001',
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=223,
  serialized_end=268,
)

_STATE = _descriptor.Descriptor(
  name='State',
  full_name='yscb_example.messages.State',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='fields', full_name='yscb_example.messages.State.fields', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='balance', full_name='yscb_example.messages.State.balance', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[_STATE_FIELDSENTRY, ],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=139,
  serialized_end=268,
)


_INSERT = _descriptor.Descriptor(
  name='Insert',
  full_name='yscb_example.messages.Insert',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='yscb_example.messages.Insert.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='state', full_name='yscb_example.messages.Insert.state', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=270,
  serialized_end=335,
)


_READ = _descriptor.Descriptor(
  name='Read',
  full_name='yscb_example.messages.Read',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='yscb_example.messages.Read.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=337,
  serialized_end=355,
)


_UPDATE_UPDATESENTRY = _descriptor.Descriptor(
  name='UpdatesEntry',
  full_name='yscb_example.messages.Update.UpdatesEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='yscb_example.messages.Update.UpdatesEntry.key', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='value', full_name='yscb_example.messages.Update.UpdatesEntry.value', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=b'8\001',
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=441,
  serialized_end=487,
)

_UPDATE = _descriptor.Descriptor(
  name='Update',
  full_name='yscb_example.messages.Update',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='yscb_example.messages.Update.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='updates', full_name='yscb_example.messages.Update.updates', index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[_UPDATE_UPDATESENTRY, ],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=358,
  serialized_end=487,
)


_RESPONSE = _descriptor.Descriptor(
  name='Response',
  full_name='yscb_example.messages.Response',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='request_id', full_name='yscb_example.messages.Response.request_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='status_code', full_name='yscb_example.messages.Response.status_code', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='message', full_name='yscb_example.messages.Response.message', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=489,
  serialized_end=579,
)

_WRAPPER.fields_by_name['message'].message_type = google_dot_protobuf_dot_any__pb2._ANY
_STATE_FIELDSENTRY.containing_type = _STATE
_STATE.fields_by_name['fields'].message_type = _STATE_FIELDSENTRY
_INSERT.fields_by_name['state'].message_type = _STATE
_UPDATE_UPDATESENTRY.containing_type = _UPDATE
_UPDATE.fields_by_name['updates'].message_type = _UPDATE_UPDATESENTRY
_RESPONSE.fields_by_name['message'].message_type = google_dot_protobuf_dot_any__pb2._ANY
DESCRIPTOR.message_types_by_name['Wrapper'] = _WRAPPER
DESCRIPTOR.message_types_by_name['State'] = _STATE
DESCRIPTOR.message_types_by_name['Insert'] = _INSERT
DESCRIPTOR.message_types_by_name['Read'] = _READ
DESCRIPTOR.message_types_by_name['Update'] = _UPDATE
DESCRIPTOR.message_types_by_name['Response'] = _RESPONSE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Wrapper = _reflection.GeneratedProtocolMessageType('Wrapper', (_message.Message,), {
  'DESCRIPTOR' : _WRAPPER,
  '__module__' : 'messages_pb2'
  # @@protoc_insertion_point(class_scope:yscb_example.messages.Wrapper)
  })
_sym_db.RegisterMessage(Wrapper)

State = _reflection.GeneratedProtocolMessageType('State', (_message.Message,), {

  'FieldsEntry' : _reflection.GeneratedProtocolMessageType('FieldsEntry', (_message.Message,), {
    'DESCRIPTOR' : _STATE_FIELDSENTRY,
    '__module__' : 'messages_pb2'
    # @@protoc_insertion_point(class_scope:yscb_example.messages.State.FieldsEntry)
    })
  ,
  'DESCRIPTOR' : _STATE,
  '__module__' : 'messages_pb2'
  # @@protoc_insertion_point(class_scope:yscb_example.messages.State)
  })
_sym_db.RegisterMessage(State)
_sym_db.RegisterMessage(State.FieldsEntry)

Insert = _reflection.GeneratedProtocolMessageType('Insert', (_message.Message,), {
  'DESCRIPTOR' : _INSERT,
  '__module__' : 'messages_pb2'
  # @@protoc_insertion_point(class_scope:yscb_example.messages.Insert)
  })
_sym_db.RegisterMessage(Insert)

Read = _reflection.GeneratedProtocolMessageType('Read', (_message.Message,), {
  'DESCRIPTOR' : _READ,
  '__module__' : 'messages_pb2'
  # @@protoc_insertion_point(class_scope:yscb_example.messages.Read)
  })
_sym_db.RegisterMessage(Read)

Update = _reflection.GeneratedProtocolMessageType('Update', (_message.Message,), {

  'UpdatesEntry' : _reflection.GeneratedProtocolMessageType('UpdatesEntry', (_message.Message,), {
    'DESCRIPTOR' : _UPDATE_UPDATESENTRY,
    '__module__' : 'messages_pb2'
    # @@protoc_insertion_point(class_scope:yscb_example.messages.Update.UpdatesEntry)
    })
  ,
  'DESCRIPTOR' : _UPDATE,
  '__module__' : 'messages_pb2'
  # @@protoc_insertion_point(class_scope:yscb_example.messages.Update)
  })
_sym_db.RegisterMessage(Update)
_sym_db.RegisterMessage(Update.UpdatesEntry)

Response = _reflection.GeneratedProtocolMessageType('Response', (_message.Message,), {
  'DESCRIPTOR' : _RESPONSE,
  '__module__' : 'messages_pb2'
  # @@protoc_insertion_point(class_scope:yscb_example.messages.Response)
  })
_sym_db.RegisterMessage(Response)


_STATE_FIELDSENTRY._options = None
_UPDATE_UPDATESENTRY._options = None
# @@protoc_insertion_point(module_scope)

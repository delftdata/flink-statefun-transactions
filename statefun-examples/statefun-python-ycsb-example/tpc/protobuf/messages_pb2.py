# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: protobuf/messages.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import any_pb2 as google_dot_protobuf_dot_any__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='protobuf/messages.proto',
  package='yscb_example.messages',
  syntax='proto3',
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\x17protobuf/messages.proto\x12\x15yscb_example.messages\x1a\x19google/protobuf/any.proto\"D\n\x07Wrapper\x12\x12\n\nrequest_id\x18\x01 \x01(\t\x12%\n\x07message\x18\x02 \x01(\x0b\x32\x14.google.protobuf.Any\"\x81\x01\n\x05State\x12\x38\n\x06\x66ields\x18\x01 \x03(\x0b\x32(.yscb_example.messages.State.FieldsEntry\x12\x0f\n\x07\x62\x61lance\x18\x02 \x01(\x05\x1a-\n\x0b\x46ieldsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"A\n\x06Insert\x12\n\n\x02id\x18\x01 \x01(\t\x12+\n\x05state\x18\x02 \x01(\x0b\x32\x1c.yscb_example.messages.State\"\x12\n\x04Read\x12\n\n\x02id\x18\x01 \x01(\t\"\x81\x01\n\x06Update\x12\n\n\x02id\x18\x01 \x01(\t\x12;\n\x07updates\x18\x02 \x03(\x0b\x32*.yscb_example.messages.Update.UpdatesEntry\x1a.\n\x0cUpdatesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"7\n\x14\x44\x65leteAndTransferAll\x12\n\n\x02id\x18\x01 \x01(\t\x12\x13\n\x0bincoming_id\x18\x02 \x01(\t\"D\n\x08Transfer\x12\x13\n\x0boutgoing_id\x18\x01 \x01(\t\x12\x13\n\x0bincoming_id\x18\x02 \x01(\t\x12\x0e\n\x06\x61mount\x18\x03 \x01(\x05\"Z\n\x08Response\x12\x12\n\nrequest_id\x18\x01 \x01(\t\x12\x13\n\x0bstatus_code\x18\x02 \x01(\x05\x12%\n\x07message\x18\x03 \x01(\x0b\x32\x14.google.protobuf.Any\"\x1b\n\tAddCredit\x12\x0e\n\x06\x61mount\x18\x01 \x01(\x05\" \n\x0eSubtractCredit\x12\x0e\n\x06\x61mount\x18\x01 \x01(\x05\"\x08\n\x06\x44\x65leteb\x06proto3'
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
  serialized_start=77,
  serialized_end=145,
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
  serialized_start=232,
  serialized_end=277,
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
  serialized_start=148,
  serialized_end=277,
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
  serialized_start=279,
  serialized_end=344,
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
  serialized_start=346,
  serialized_end=364,
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
  serialized_start=450,
  serialized_end=496,
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
  serialized_start=367,
  serialized_end=496,
)


_DELETEANDTRANSFERALL = _descriptor.Descriptor(
  name='DeleteAndTransferAll',
  full_name='yscb_example.messages.DeleteAndTransferAll',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='yscb_example.messages.DeleteAndTransferAll.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='incoming_id', full_name='yscb_example.messages.DeleteAndTransferAll.incoming_id', index=1,
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
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=498,
  serialized_end=553,
)


_TRANSFER = _descriptor.Descriptor(
  name='Transfer',
  full_name='yscb_example.messages.Transfer',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='outgoing_id', full_name='yscb_example.messages.Transfer.outgoing_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='incoming_id', full_name='yscb_example.messages.Transfer.incoming_id', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='amount', full_name='yscb_example.messages.Transfer.amount', index=2,
      number=3, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
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
  serialized_start=555,
  serialized_end=623,
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
  serialized_start=625,
  serialized_end=715,
)


_ADDCREDIT = _descriptor.Descriptor(
  name='AddCredit',
  full_name='yscb_example.messages.AddCredit',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='amount', full_name='yscb_example.messages.AddCredit.amount', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
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
  serialized_start=717,
  serialized_end=744,
)


_SUBTRACTCREDIT = _descriptor.Descriptor(
  name='SubtractCredit',
  full_name='yscb_example.messages.SubtractCredit',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='amount', full_name='yscb_example.messages.SubtractCredit.amount', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
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
  serialized_start=746,
  serialized_end=778,
)


_DELETE = _descriptor.Descriptor(
  name='Delete',
  full_name='yscb_example.messages.Delete',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
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
  serialized_start=780,
  serialized_end=788,
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
DESCRIPTOR.message_types_by_name['DeleteAndTransferAll'] = _DELETEANDTRANSFERALL
DESCRIPTOR.message_types_by_name['Transfer'] = _TRANSFER
DESCRIPTOR.message_types_by_name['Response'] = _RESPONSE
DESCRIPTOR.message_types_by_name['AddCredit'] = _ADDCREDIT
DESCRIPTOR.message_types_by_name['SubtractCredit'] = _SUBTRACTCREDIT
DESCRIPTOR.message_types_by_name['Delete'] = _DELETE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Wrapper = _reflection.GeneratedProtocolMessageType('Wrapper', (_message.Message,), {
  'DESCRIPTOR' : _WRAPPER,
  '__module__' : 'protobuf.messages_pb2'
  # @@protoc_insertion_point(class_scope:yscb_example.messages.Wrapper)
  })
_sym_db.RegisterMessage(Wrapper)

State = _reflection.GeneratedProtocolMessageType('State', (_message.Message,), {

  'FieldsEntry' : _reflection.GeneratedProtocolMessageType('FieldsEntry', (_message.Message,), {
    'DESCRIPTOR' : _STATE_FIELDSENTRY,
    '__module__' : 'protobuf.messages_pb2'
    # @@protoc_insertion_point(class_scope:yscb_example.messages.State.FieldsEntry)
    })
  ,
  'DESCRIPTOR' : _STATE,
  '__module__' : 'protobuf.messages_pb2'
  # @@protoc_insertion_point(class_scope:yscb_example.messages.State)
  })
_sym_db.RegisterMessage(State)
_sym_db.RegisterMessage(State.FieldsEntry)

Insert = _reflection.GeneratedProtocolMessageType('Insert', (_message.Message,), {
  'DESCRIPTOR' : _INSERT,
  '__module__' : 'protobuf.messages_pb2'
  # @@protoc_insertion_point(class_scope:yscb_example.messages.Insert)
  })
_sym_db.RegisterMessage(Insert)

Read = _reflection.GeneratedProtocolMessageType('Read', (_message.Message,), {
  'DESCRIPTOR' : _READ,
  '__module__' : 'protobuf.messages_pb2'
  # @@protoc_insertion_point(class_scope:yscb_example.messages.Read)
  })
_sym_db.RegisterMessage(Read)

Update = _reflection.GeneratedProtocolMessageType('Update', (_message.Message,), {

  'UpdatesEntry' : _reflection.GeneratedProtocolMessageType('UpdatesEntry', (_message.Message,), {
    'DESCRIPTOR' : _UPDATE_UPDATESENTRY,
    '__module__' : 'protobuf.messages_pb2'
    # @@protoc_insertion_point(class_scope:yscb_example.messages.Update.UpdatesEntry)
    })
  ,
  'DESCRIPTOR' : _UPDATE,
  '__module__' : 'protobuf.messages_pb2'
  # @@protoc_insertion_point(class_scope:yscb_example.messages.Update)
  })
_sym_db.RegisterMessage(Update)
_sym_db.RegisterMessage(Update.UpdatesEntry)

DeleteAndTransferAll = _reflection.GeneratedProtocolMessageType('DeleteAndTransferAll', (_message.Message,), {
  'DESCRIPTOR' : _DELETEANDTRANSFERALL,
  '__module__' : 'protobuf.messages_pb2'
  # @@protoc_insertion_point(class_scope:yscb_example.messages.DeleteAndTransferAll)
  })
_sym_db.RegisterMessage(DeleteAndTransferAll)

Transfer = _reflection.GeneratedProtocolMessageType('Transfer', (_message.Message,), {
  'DESCRIPTOR' : _TRANSFER,
  '__module__' : 'protobuf.messages_pb2'
  # @@protoc_insertion_point(class_scope:yscb_example.messages.Transfer)
  })
_sym_db.RegisterMessage(Transfer)

Response = _reflection.GeneratedProtocolMessageType('Response', (_message.Message,), {
  'DESCRIPTOR' : _RESPONSE,
  '__module__' : 'protobuf.messages_pb2'
  # @@protoc_insertion_point(class_scope:yscb_example.messages.Response)
  })
_sym_db.RegisterMessage(Response)

AddCredit = _reflection.GeneratedProtocolMessageType('AddCredit', (_message.Message,), {
  'DESCRIPTOR' : _ADDCREDIT,
  '__module__' : 'protobuf.messages_pb2'
  # @@protoc_insertion_point(class_scope:yscb_example.messages.AddCredit)
  })
_sym_db.RegisterMessage(AddCredit)

SubtractCredit = _reflection.GeneratedProtocolMessageType('SubtractCredit', (_message.Message,), {
  'DESCRIPTOR' : _SUBTRACTCREDIT,
  '__module__' : 'protobuf.messages_pb2'
  # @@protoc_insertion_point(class_scope:yscb_example.messages.SubtractCredit)
  })
_sym_db.RegisterMessage(SubtractCredit)

Delete = _reflection.GeneratedProtocolMessageType('Delete', (_message.Message,), {
  'DESCRIPTOR' : _DELETE,
  '__module__' : 'protobuf.messages_pb2'
  # @@protoc_insertion_point(class_scope:yscb_example.messages.Delete)
  })
_sym_db.RegisterMessage(Delete)


_STATE_FIELDSENTRY._options = None
_UPDATE_UPDATESENTRY._options = None
# @@protoc_insertion_point(module_scope)

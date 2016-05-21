"""
Pure python read and write for the HDF5 file format.

Karim Bahgat 2016
"""

import struct
import datetime


# Main user interface
class HDF5(object):
    def __init__(self, filepath=None):
        if filepath:
            self.filepath = filepath
            self.fileobj = _FileWrap(open(self.filepath, "rb"))
            self._read_file_metadata()
            #self._read_file_infrastructure()
            
        else:
            raise NotImplementedError("Only reading is currently supported")

    def _read_file_metadata(self):
        # level 0A
        self.superblock = _SuperBlock(self.fileobj)
        # level 0B
        #self.file_driver_info = _DriverInformationBlock(self.fileobj)
        # level 0C
        #self.superblock_extension = _SuperBlockExtension(self.fileobj)

    def _read_file_infrastructure(self):
        # ...
        pass

    def get_root(self):
        return self.superblock.get_root()


# High level abstract data model objects contained within a HDF5 file
# Link: https://www.hdfgroup.org/HDF5/doc/UG/HDF5_Users_Guide-Responsive%20HTML5/index.html#t=HDF5_Users_Guide%2FDataModelAndFileStructure%2FThe_HDF5_Data_Model_and_File_Structure.htm
# ...


# "Low level" objects as they actually exist on disk, for reading and writing
# Link: https://www.hdfgroup.org/HDF5/doc/H5.format.html

def _bitflag(rawbyte, index):
    # loworder first
    # From: http://stackoverflow.com/questions/2576712/using-python-how-can-i-read-the-bits-in-a-byte
    i, j = divmod(index, 8)

    if ord(rawbyte[i]) & (1 << j):
        return 1
    else:
        return 0


def _bitfield(rawbyte, startindex, endindex):
    value = 0
    mult = 1

    for i in range(startindex,endindex+1):
        value += _bitflag(rawbyte, i) * mult
        mult *= 2

    return value
    
        

class _FileWrap(object):
    endian = "<"
    
    def __init__(self, fileobj):
        self.fileobj = fileobj

    # Basic reading

    def read_struct_type(self, struct_type, n):
        fmt = self.endian + bytes(n) + struct_type
        size = struct.calcsize(fmt)
        raw = self.read_bytes(size)
        value = struct.unpack(fmt, raw)
        if len(value) == 1:
            value = value[0]
        return value

    def read_bytes(self, n):
        raw = self.fileobj.read(n)
        return raw

    def read_unknown_nr(self, size, n):
        typ = {1:"B",2:"H",4:"I",8:"Q"}[size]
        return self.read_struct_type(typ, n)

    # Positioning

    def tell(self):
        return self.fileobj.tell()

    def seek(self, pos):
        self.fileobj.seek(pos)

    def set_checkpoint(self):
        self.pos = self.fileobj.tell()

    def return_to_checkpoint(self):
        self.fileobj.seek(self.pos, 0) # absolute position
        

class _SuperBlock(object):
    def __init__(self, fileobj=None, **kwargs):
        if fileobj:
            self.fileobj = fileobj
            self.read()
        else:
            # set superblock attrs from kwargs...
            raise NotImplementedError("Building from scratch not yet supported")

    def __str__(self):
        from pprint import pformat
        return "----- \n %r \n %s"%(self, pformat(self.__dict__, indent=4) )
    
    def read(self):
        if not hasattr(self, "fileobj"):
            raise Exception("Must be initiated with a fileobj in order to call read()")
        
        self._read_format_signature()
        self._read_version()

        if self.version in (0,1):
            self._read_freespace_version()
            self._read_rootsymtable_version()
            self._read_reserved(1)
            
            self._read_sharedheadermsg_version()
            self._read_offset_size()
            self._read_length_size()
            self._read_reserved(1)

            self._read_groupleafnodek()
            self._read_groupinternalnodek()
            
            self._read_fileconsflags()

            if self.version == 1:
                self._read_indexedstorageinternalnodek()
                self._read_reserved(2)

            self._read_base_address()
            self._read_freespace_address()
            self._read_end_address()
            self._read_driver_address()

            self._read_rootsymtable()

        elif self.version in (2,3):
            self._read_offset_size()
            self._read_length_size()
            self._read_fileconsflags()

            self._read_base_address()
            self._read_superblockext_address()

            if hex(self.superblockext_address) != "0xffffffffffffffffL": # TODO: hacky, will only work for 8byte addresses
                self._read_extension() # extends with additional superblock options

            self._read_end_address()
            self._read_rootheader_address()

            self._read_superblock_checksum()

    def get_root(self):
        if self.version in (0,1):
            raise NotImplementedError("Data access for version 0 and 1 not yet supported")

        elif self.version in (2,3):
            offset = self.base_address + self.rootheader_address
            self.fileobj.seek(offset)
            root = _ObjectHeader(parent=self, fileobj=self.fileobj)
            return root

    # internal

    def _read_extension(self):
        pass

    def _read_format_signature(self):
        self.fileobj.seek(0)
        formatsign = self.fileobj.read_bytes(8)

        # keep looking for formatsign if not found
        byteoffset = 512
        while formatsign != '\x89HDF\r\n\x1a\n':
            self.fileobj.seek(byteoffset)
            formatsign = self.fileobj.read_bytes(8)
            
            # skip to next byteoffset at multiples of 2
            byteoffset *= 2

        self.format_signature = formatsign

    def _read_version(self):
        "Superblock version"
        self.version = self.fileobj.read_struct_type("B", 1) # singlebyte unsigned nr

    def _read_freespace_version(self):
        "Free space version"
        self.freespace_version = self.fileobj.read_struct_type("B", 1) # singlebyte unsigned nr

    def _read_rootsymtable_version(self):
        "Root group symbol table entry version"
        self.rootsymtable_version = self.fileobj.read_struct_type("B", 1) # singlebyte unsigned nr

    def _read_reserved(self, n):
        val = self.fileobj.read_struct_type("B", n) # singlebyte unsigned nr
        assert val == 0 

    def _read_sharedheadermsg_version(self):
        "Shared header message version"
        self.sharedheadermsg_version = self.fileobj.read_struct_type("B", 1) # singlebyte unsigned nr                

    def _read_offset_size(self):
        "Size of offsets"
        self.offset_size = self.fileobj.read_struct_type("B", 1) # singlebyte unsigned nr                

    def _read_length_size(self):
        "Size of lengths"
        self.length_size = self.fileobj.read_struct_type("B", 1) # singlebyte unsigned nr

    def _read_fileconsflags(self):
        "File consistency flags (aka current readwrite access status)"
        if self.version in (0,1):
            # skip, 4 bytes
            _ = self.fileobj.read_struct_type("B", 4) 
            self.fileconsflags = None

        elif self.version == 2:
            # skip, reduced to a single byte
            _ = self.fileobj.read_struct_type("B", 1) 
            self.fileconsflags = None
            
        elif self.version == 3:
            raw = self.fileobj.read_bytes(1)
            self.fileconsflags = dict(writeaccess=_bitflag(raw,0),
                                      writemultireadaccess=_bitflag(raw,2))

    def _read_groupleafnodek(self):
        "Group Leaf Node K"
        self.groupleafnodek = self.fileobj.read_struct_type("B", 1) # singlebyte unsigned nr
        assert self.groupleafnodek > 0

    def _read_groupinternalnodek(self):
        "Group Internal Node K"
        self.groupinternalnodek = self.fileobj.read_struct_type("B", 1) # singlebyte unsigned nr
        assert self.groupinternalnodek > 0

    def _read_indexedstorageinternalnodek(self):
        "Indexed Storage Internal Node K"
        self.indexedstorageinternalnodek = self.fileobj.read_struct_type("H", 1) # 2byte unsigned short
        assert self.indexedstorageinternalnodek > 0

    def _read_base_address(self):
        "Start of superblock"
        self.base_address = self.fileobj.read_unknown_nr(self.offset_size, 1)

    def _read_freespace_address(self):
        "Start of freespace"
        self.freespace_address = self.fileobj.read_unknown_nr(self.offset_size, 1)

    def _read_end_address(self):
        "End of file"
        self.end_address = self.fileobj.read_unknown_nr(self.offset_size, 1)
        
    def _read_driver_address(self):
        "Start of driver information"
        self.driver_address = self.fileobj.read_unknown_nr(self.offset_size, 1)

    def _read_superblockext_address(self):
        "Start of superblock extension"
        self.superblockext_address = self.fileobj.read_unknown_nr(self.offset_size, 1)
        
    def _read_rootsymtable(self):
        "Root group symbol table entry"
        pass

    def _read_rootheader_address(self):
        "Start of Root group object header"
        self.rootheader_address = self.fileobj.read_unknown_nr(self.offset_size, 1)

    def _read_superblock_checksum(self):
        # TODO: Not sure how to parse checksum...
        self.superblock_checksum = self.fileobj.read_struct_type("s",4)



class _BTreeNode(object):
    def __init__(self):
        pass


class _HeapBlock(object):
    def __init__(self):
        pass


class _ObjectHeader(object):
    def __init__(self, parent, fileobj=None, **kwargs):
        "If root object, parent must be the superblock object, otherwise just a parent object"
        self.parent = parent
        
        if fileobj:
            self.fileobj = fileobj
            self.read()
        else:
            # set superblock attrs from kwargs...
            raise NotImplementedError("Building from scratch not yet supported")

    def __str__(self):
        from pprint import pformat
        return "----- \n %r \n %s"%(self, pformat(self.__dict__, indent=4) )

    def get_root(self):
        obj = self
        while hasattr(obj, "parent") and not isinstance(obj.parent, _SuperBlock):
            obj = obj.parent

        return obj
    
    def read(self):
        if not hasattr(self, "fileobj"):
            raise Exception("Must be initiated with a fileobj in order to call read()")

        self._read_prefix()
        self._read_messages()

    # internal

    def _read_prefix(self):
        self.prefix = _ObjectHeaderPrefix(parent=self, fileobj=self.fileobj)

    def _read_messages(self):
        self.messages = self.prefix._read_messages()


class _ObjectData(object):
    def __init__(self):
        pass


class _FreeSpace(object):
    def __init__(self):
        pass


# object header components and message types
# ...

class _ObjectHeaderPrefix(object):
    def __init__(self, parent, fileobj=None, **kwargs):
        self.parent = parent
        
        if fileobj:
            self.fileobj = fileobj
            self.read()
        else:
            # set superblock attrs from kwargs...
            raise NotImplementedError("Building from scratch not yet supported")

    def __str__(self):
        from pprint import pformat
        return "----- \n %r \n %s"%(self, pformat(self.__dict__, indent=4) )

    def get_root(self):
        obj = self
        while hasattr(obj, "parent") and not isinstance(obj.parent, _SuperBlock):
            obj = obj.parent

        return obj
    
    def read(self):
        if not hasattr(self, "fileobj"):
            raise Exception("Must be initiated with a fileobj in order to call read()")

        self._read_version()

        if self.version == 1:
            raise NotImplementedError("Version 1 of objectheader prefix not yet supported")

        elif self.version == 2:
            self._read_flags()

            if self.flags["storetimes"]:
                self._read_accesstime()
                self._read_modiftime()
                self._read_changetime()
                self._read_birthtime()

            if self.flags["storenondefattrchange"]:
                self._read_maxcompattr()
                self._read_maxdensattr()

            self._read_chunk0size()

            # start of message headers (is not read automatically, has to be read via _read_messages())
            self._chunkstart = self.fileobj.tell()

            # skip the gap (seek to end - 4bytes for checksum)
            self.fileobj.seek(self._chunkstart + self.chunk0size - 4) 
            self._read_checksum()

        # ...

    def _read_version(self):
        # test for signature which is a sign of version 2
        self.fileobj.set_checkpoint()
        signa = self.fileobj.read_struct_type("s", 4)

        if signa == "OHDR":
            # v2
            self.signature = signa
            self.version = self.fileobj.read_struct_type("B", 1)
            assert self.version == 2

        else:
            # v1
            self.fileobj.return_to_checkpoint()
            self.version = self.fileobj.read_struct_type("B", 1)
            assert self.version == 1

    def _read_flags(self):
        # these are supposed to be 4 different bits that can be turned on or off
        # TODO: how to extract that from the single byte and represent in Python?
        # ...
        raw = self.fileobj.read_bytes(1)
        self.flags = dict(chunksizesize={0:1, 1:2, 2:4, 3:8}[_bitfield(raw,0,1)],
                          trackattrorder=_bitflag(raw,2),
                          indexattrorder=_bitflag(raw,3),
                          storenondefattrchange=_bitflag(raw,4),
                          storetimes=_bitflag(raw,5),
                          )

    def _read_accesstime(self):
        secs = self.fileobj.read_struct_type("I", 1) # 32 bit unsigned int
        self.accesstime = datetime.datetime.fromtimestamp(secs)

    def _read_modiftime(self):
        secs = self.fileobj.read_struct_type("I", 1) # 32 bit unsigned int
        self.modiftime = datetime.datetime.fromtimestamp(secs)

    def _read_changetime(self):
        secs = self.fileobj.read_struct_type("I", 1) # 32 bit unsigned int
        self.changetime = datetime.datetime.fromtimestamp(secs)

    def _read_birthtime(self):
        secs = self.fileobj.read_struct_type("I", 1) # 32 bit unsigned int
        self.birthtime = datetime.datetime.fromtimestamp(secs)

    def _read_maxcompattr(self):
        self.maxcompattr = self.fileobj.read_struct_type("H", 1) # 2-byte nr

    def _read_maxdensattr(self):
        self.maxdensattr = self.fileobj.read_struct_type("H", 1) # 2-byte nr

    def _read_chunk0size(self):
        self.chunk0size = self.fileobj.read_unknown_nr(self.flags["chunksizesize"], 1)

    def _read_messages(self):
        messages = list()
        
        if self.version == 1:
            raise NotImplementedError("Reading messages from object header prefix version 1 not yet implemented")

        elif self.version == 2:
            self.fileobj.seek(self._chunkstart)
            
            while (self.fileobj.tell() - self._chunkstart) < self.chunk0size:
                msg = dict()
                # msg type
                msg["msgtype"] = self.fileobj.read_struct_type("B", 1) # 1-byte nr
                # msg data size
                msg["msgdatasize"] = self.fileobj.read_struct_type("H", 1) # 2-byte nr
                # msg flags
                msg["msgflags"] = self._read_msgflags()
                # msg creation order
                if self.flags["trackattrorder"]:
                    msg["msgorder"] = self.fileobj.read_struct_type("H", 1) # 2-byte nr
                # msg data
                msg["msgdata"] = self._read_msgdata(msg)

                messages.append(msg)

        return messages

    def _read_msgflags(self):
        raw = self.fileobj.read_bytes(1)
        return dict(const=_bitflag(raw, 0),
                    sharestore=_bitflag(raw, 1),
                    noshare=_bitflag(raw, 2),
                    skipfail=_bitflag(raw, 3),
                    markfail=_bitflag(raw, 4),
                    violfail=_bitflag(raw, 5),
                    sharable=_bitflag(raw, 6),
                    alwaysfail=_bitflag(raw, 7),
                    )

    def _read_msgdata(self, msg):
        if msg["msgflags"]["sharestore"]:
            data = _SharedMessage(parent=self, fileobj=self.fileobj)
        else:
            typ = msg["msgtype"]
            if typ == 0:
                # skip the nil msg
                self.fileobj.read_bytes(msg["msgdatasize"])
                data = None
            elif typ == 1:
                data = _DataspaceMessage(parent=self, fileobj=self.fileobj)
            elif typ == 2:
                data = _LinkInfoMessage(parent=self, fileobj=self.fileobj)
            elif typ == 3:
                data = _DataTypeMessage(parent=self, fileobj=self.fileobj)
            elif typ == 6:
                data = _LinkMessage(parent=self, fileobj=self.fileobj)
            elif typ == 10:
                data = _HeaderContMessage(parent=self, fileobj=self.fileobj)
            else:
                raise NotImplementedError("Message type %s not yet supported" % typ)

            return data

    def _read_checksum(self):
        # TODO: Not sure how to parse checksum...
        self.checksum = self.fileobj.read_struct_type("s",4)



class _SharedMessage(object):
    def __init__(self, parent, fileobj=None):
        self.parent = parent
        
        if fileobj:
            self.fileobj = fileobj
            self.read()
        else:
            # set superblock attrs from kwargs...
            raise NotImplementedError("Building from scratch not yet supported")

    def __str__(self):
        from pprint import pformat
        return "----- \n %r \n %s"%(self, pformat(self.__dict__, indent=4) )

    def get_root(self):
        obj = self
        while hasattr(obj, "parent") and not isinstance(obj.parent, _SuperBlock):
            obj = obj.parent

        return obj

    def read(self):
        if not hasattr(self, "fileobj"):
            raise Exception("Must be initiated with a fileobj in order to call read()")

        self._read_version()

        if self.version == 1:
            self._read_type()
            self._read_reserved(2)

            self._read_reserved(4)

            self._read_address()

        elif self.version == 2:
            self._read_type()
            self._read_address()

        elif self.version == 3:
            self._read_type()
            self._read_location()

    def _read_version(self):
        self.version = self.fileobj.read_struct_type("B", 1)

    def _read_type(self):
        self.type = self.fileobj.read_struct_type("B", 1)

    def _read_reserved(self, n):
        self.fileobj.read_bytes(n)

    def _read_address(self):
        superblock = self.get_root().parent
        self.address = self.fileobj.read_unknown_nr(superblock.offset_size, 1)

    def _read_location(self):
        if self.type == 1: # in shared heap
            self.location = self.fileobj.read_struct_type("Q", 1) # 8-byte int
        else:
            superblock = self.get_root().parent
            self.location = self.fileobj.read_unknown_nr(superblock.offset_size, 1)

                       




class _DataspaceMessage(object):
    def __init__(self, parent, fileobj=None):
        self.parent = parent
        
        if fileobj:
            self.fileobj = fileobj
            self.read()
        else:
            # set superblock attrs from kwargs...
            raise NotImplementedError("Building from scratch not yet supported")

    def __str__(self):
        from pprint import pformat
        return "----- \n %r \n %s"%(self, pformat(self.__dict__, indent=4) )

    def get_root(self):
        obj = self
        while hasattr(obj, "parent") and not isinstance(obj.parent, _SuperBlock):
            obj = obj.parent

        return obj

    def read(self):
        if not hasattr(self, "fileobj"):
            raise Exception("Must be initiated with a fileobj in order to call read()")

        self._read_version()
        
        if self.version == 1:
            self._read_dimensionality()
            self._read_flags()
            self._read_reserved(1)

            self._read_reserved(4)

            self.dimsizes = (self._read_dimension_size() for _ in range(self.dimensionality))

            if self.flags["maxdims"]:
                self.maxdimsizes = (self._read_maxdimsize() for _ in range(self.dimensionality))
            if self.flags["permutindices"]:
                self.permutindices = (self._read_permuation_index() for _ in range(self.dimensionality))
            
    def _read_version(self):
        self.version = self.fileobj.read_struct_type("B", 1) # 1-byte nr

    def _read_dimensionality(self):
        self.dimensionality = self.fileobj.read_struct_type("B", 1) # 1-byte nr

    def _read_flags(self):
        raw = self.fileobj.read_bytes(1)
        self.flags = dict(maxdims=_bitflag(raw, 0),
                          permutindic=_bitflag(raw, 1))

    def _read_reserved(self, n):
        # skip a single reserved byte
        self.fileobj.read_bytes(n)

    def _read_dimensionsize(self):
        superblock = self.get_root().parent
        return self.fileobj.read_unknown_nr(superblock.length_size, 1) 

    def _read_maxdimsize(self):
        superblock = self.get_root().parent
        return self.fileobj.read_unknown_nr(superblock.length_size, 1)

    def _read_permutation_index(self):
        superblock = self.get_root().parent
        return self.fileobj.read_unknown_nr(superblock.length_size, 1) 


class _LinkInfoMessage(object):
    def __init__(self, parent, fileobj=None):
        self.parent = parent
        
        if fileobj:
            self.fileobj = fileobj
            self.read()
        else:
            # set superblock attrs from kwargs...
            raise NotImplementedError("Building from scratch not yet supported")

    def __str__(self):
        from pprint import pformat
        return "----- \n %r \n %s"%(self, pformat(self.__dict__, indent=4) )

    def get_root(self):
        obj = self
        while hasattr(obj, "parent") and not isinstance(obj.parent, _SuperBlock):
            obj = obj.parent

        return obj

    def read(self):
        if not hasattr(self, "fileobj"):
            raise Exception("Must be initiated with a fileobj in order to call read()")

        self._read_version()
        
        if self.version == 0:
            self._read_flags()
            
            if self.flags["trackorder"]:
                self._read_maxorderindex()

            self._read_fractheap_address()
            self._read_nameindex_v2btree_address()

            if self.flags["indexorder"]:
                self._read_orderindex_v2btree_address()

        else:
            raise Exception("This version does not exist")
            
    def _read_version(self):
        self.version = self.fileobj.read_struct_type("B", 1)

    def _read_flags(self):
        raw = self.fileobj.read_bytes(1)
        self.flags = dict(trackorder=_bitflag(raw, 0),
                          indexorder=_bitflag(raw, 1))

    def _read_maxorderindex(self):
        self.maxorderindex = self.fileobj.read_struct_type("Q", 1) # 64-bit int

    def _read_fractheap_address(self):
        superblock = self.get_root().parent
        self.fractheap_address = self.fileobj.read_unknown_nr(superblock.offset_size, 1)

    def _read_nameindex_v2btree_address(self):
        superblock = self.get_root().parent
        self.nameindex_v2btree_address = self.fileobj.read_unknown_nr(superblock.offset_size, 1)

    def _read_orderindex_v2btree_address(self):
        superblock = self.get_root().parent
        self.orderindex_v2btree_address = self.fileobj.read_unknown_nr(superblock.offset_size, 1)


class _LinkMessage(object):
    def __init__(self, parent, fileobj=None):
        self.parent = parent
        
        if fileobj:
            self.fileobj = fileobj
            self.read()
        else:
            # set superblock attrs from kwargs...
            raise NotImplementedError("Building from scratch not yet supported")

    def __str__(self):
        from pprint import pformat
        return "----- \n %r \n %s"%(self, pformat(self.__dict__, indent=4) )

    def get_root(self):
        obj = self
        while hasattr(obj, "parent") and not isinstance(obj.parent, _SuperBlock):
            obj = obj.parent

        return obj

    def read(self):
        if not hasattr(self, "fileobj"):
            raise Exception("Must be initiated with a fileobj in order to call read()")

        self._read_version()
        
        if self.version == 1:
            self._read_flags()

            if self.flags["linktype"]:
                self._read_linktype()
            else:
                self.linktype = "hard"

            if self.flags["creationorder"]:
                self._read_creationorder()

            if self.flags["namecharset"]:
                self._read_namecharset()
            else:
                self.namecharset = "ascii"

            self._read_namelength()
            self._read_name()

            self._read_link()

        else:
            raise Exception("This version does not exist")
            
    def _read_version(self):
        self.version = self.fileobj.read_struct_type("B", 1)

    def _read_flags(self):
        raw = self.fileobj.read_bytes(1)
        self.flags = dict(namelengthsize={0:1, 1:2, 2:4, 3:8}[_bitfield(raw, 0, 1)],
                          creationorder=_bitflag(raw, 2),
                          linktype=_bitflag(raw, 3),
                          namecharset=_bitflag(raw, 4),
                          )

    def _read_linktype(self):
        # TODO: allow userdefined linktypes, 65-255
        val = self.fileobj.read_struct_type("B", 1)
        self.linktype = {0:"hard", 1:"soft", 64:"external"}[val]

    def _read_creationorder(self):
        self.creationorder = self.fileobj.read_struct_type("Q", 1)

    def _read_namecharset(self):
        val = self.fileobj.read_struct_type("B", 1)
        self.namecharset = {0:"ascii", 1:"utf8"}[val]

    def _read_namelength(self):
        self.namelength = self.fileobj.read_unknown_nr(self.flags["namelengthsize"], 1)

    def _read_name(self):
        self.name = self.fileobj.read_struct_type("s", self.namelength).decode(self.namecharset)

    def _read_link(self):
        if self.linktype == "hard":
            superblock = self.get_root().parent
            offset = self.fileobj.read_unknown_nr(superblock.offset_size, 1)

            pos = self.fileobj.tell() # this might go down a rabbithole of nested objects...
            
            self.fileobj.seek(superblock.base_address + offset)
            self.link = _ObjectHeader(parent=self, fileobj=self.fileobj)

            self.fileobj.seek(pos) # so remember to return to previous position after
            
        elif self.linktype == "soft":
            length = self.fileobj.read_struct_type("H", 1) # 2-byte nr
            self.link = self.fileobj.read_struct_type("s", length)

        elif self.linktype == "external":
            #length = self.fileobj.read_struct_type("H", 1) # 2-byte nr
            raise NotImplementedError("External link types not yet supported")



class _HeaderContMessage(object):
    def __init__(self, parent, fileobj=None):
        self.parent = parent
        
        if fileobj:
            self.fileobj = fileobj
            self.read()
        else:
            # set superblock attrs from kwargs...
            raise NotImplementedError("Building from scratch not yet supported")

    def __str__(self):
        from pprint import pformat
        return "----- \n %r \n %s"%(self, pformat(self.__dict__, indent=4) )

    def get_root(self):
        obj = self
        while hasattr(obj, "parent") and not isinstance(obj.parent, _SuperBlock):
            obj = obj.parent

        return obj

    def read(self):
        if not hasattr(self, "fileobj"):
            raise Exception("Must be initiated with a fileobj in order to call read()")

        self._read_offset()
        self._read_length()

    def _read_offset(self):
        # TODO: hacky set to 1 byte, should use suplerblock offset size but becomes larger than its own datasize as stated in meta dict
        superblock = self.get_root().parent
        self.offset = self.fileobj.read_unknown_nr(1, 1) #superblock.offset_size, 1)

    def _read_length(self):
        # TODO: hacky set to 1 byte, should use suplerblock offset size but becomes larger than its own datasize as stated in meta dict
        superblock = self.get_root().parent
        self.length = self.fileobj.read_unknown_nr(1, 1) #superblock.length_size, 1)





class _DataTypeMessage(object):
    def __init__(self, parent, fileobj=None):
        self.parent = parent
        
        if fileobj:
            self.fileobj = fileobj
            self.read()
        else:
            # set superblock attrs from kwargs...
            raise NotImplementedError("Building from scratch not yet supported")

    def __str__(self):
        from pprint import pformat
        return "----- \n %r \n %s"%(self, pformat(self.__dict__, indent=4) )

    def get_root(self):
        obj = self
        while hasattr(obj, "parent") and not isinstance(obj.parent, _SuperBlock):
            obj = obj.parent

        return obj

    def read(self):
        if not hasattr(self, "fileobj"):
            raise Exception("Must be initiated with a fileobj in order to call read()")

        self._read_class_and_version()
        self._read_bitfields()
        self._read_size()
        self._read_properties()

    def _read_class_and_version(self):
        raw = self.fileobj.read_bytes(1)
        
        self.version = _bitfield(raw, 0, 3)
        assert 1 <= self.version <= 3

        self.classtype = {0: "fixpoint",
                          1: "floatpoint",
                          2: "time",
                          3: "string",
                          4: "bitfield",
                          5: "opaque",
                          6: "compound",
                          7: "reference",
                          8: "enumerated",
                          9: "varlength",
                          10: "array",
                          } [_bitfield(raw, 4, 7)]

    def _read_bitfields(self):
        raw = self.fileobj.read_bytes(3)
        self.bitfields = [_bitflag(raw, i) for i in range(23)]

##        if self.classtype == "fixpoint":
##            self.bitfields = dict()[_bitflag(raw, x)]
##
##        elif self.classtype == "floatpoint":
##            pass

    def _read_size(self):
        self.size = self.fileobj.read_unknown_nr(4, 1)

    def _read_properties(self):
        pass


            

###
    
if __name__ == "__main__":
    testfile = HDF5("C:\Users\kimo\Downloads\spei01.nc")
    
    print testfile.superblock
    
    root = testfile.get_root()
    print root
    
    print root.prefix

    for msg in root.messages:
        print "msg:",msg["msgdata"]









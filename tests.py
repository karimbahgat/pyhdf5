
from pyhdf5 import HDF5

testfile = HDF5(r"C:\Users\kimo\Downloads\spei01.nc") #"testfiles/spei01.nc")

print testfile.superblock

root = testfile.get_root()
print root

print root.prefix

for msg in root.messages:
    print "msg:",msg["msgdata"]
    
    if hasattr(msg["msgdata"], "link"):
        print msg["msgdata"].link

        for submsg in msg["msgdata"].link.messages:
            if submsg["msgtype"] == 8:
                print "??????????????????"
                print submsg["msgdata"].read_data()
                










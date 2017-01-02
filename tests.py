
##import sys
##sys.path.insert(0, "")
##
##import pyfive
##print pyfive
##
##testfile = pyfive.File(r"C:\Users\kimo\Downloads\spei01.nc")
##testfile["spei"]._dataobjects.get_data()
##fdsdf


from pyhdf5 import HDF5

testfile = HDF5(r"C:\Users\kimo\Downloads\spei01.nc") #"testfiles/spei01.nc")

print "SUPERBLOCK",testfile.superblock

root = testfile.get_root()
print "ROOT",root

print "PREFIX",root.prefix

for msg in root.messages:
    print "msg:",msg["msgdata"]
    
    if hasattr(msg["msgdata"], "link"):
        print "LINK:", msg["msgdata"].name, msg["msgdata"].link

        for submsg in msg["msgdata"].link.messages:
            if submsg["msgtype"] == 1:
                print "DATASPACE:", submsg["msgdata"]
            elif submsg["msgtype"] == 8:
                print "DATALAYOUT:", submsg["msgdata"]
                dat = submsg["msgdata"].read_data()
                print len(dat), ":", str(dat)[:300]
                










# -*- coding: utf-8 -*-
import os
import click
import logging
from zipfile import ZipFile

    
dblquotes = re.compile("\x93|\x94")
sglquotes = re.compile("\x91|\x92")
space = re.compile("\x97|\x95")
oe = re.compile("\x9c")
dash = re.compile("-\xad|\xad|\xad-")
nochar = re.compile("\x9d|\x85|\x96")

akn = "../../data/external/AKN_dail_copy.zip"
z = ZipFile(akn)

testxml = "dail/AK-dail-1922-01-06.xml"
for fn in z.filelist:
    if fn.filename.startswith("dail/AK-dail"):
        with z.open(fn) as x:
            xml = x.read().decode("utf-8", "strict")
        xml = dblquotes.sub('"', xml)
        xml = sglquotes.sub("'", xml)
        xml = space.sub(" ", xml)
        xml = xml.replace("\x9c", "oe").replace("\x8a", "Š").replace("\x8a", "Š".lower())
        xml = dash.sub("-", xml)
        xml = nochar.sub("", xml)
        with open("../../data/external/"+fn.filename, "wb") as f:
            f.write(xml.encode("utf-8"))

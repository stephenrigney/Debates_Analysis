# -*- coding: utf-8 -*-
import os
import click
import logging
import spacy
import re
from lxml import etree
from zipfile import ZipFile
from bz2 import BZ2File
from dotenv import find_dotenv, load_dotenv

NS = {"akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD13"}

def extract_paragraphs(input_filepath, output_dirpath, start_year, end_year, interval, nlp):
    '''Input is zipped Akoma Ntoso XML of type debateRecord.
    Output is a compressed (BZ2) file for each period from year to year+interval-1 years in the range from start_year to end _year.
    if interval is not set, the period is start_year to end_year-1
    '''

    z = ZipFile(input_filepath)
    cumulative_para_count = 0
    if interval is None: interval = end_year-start_year
    for year in range(start_year, end_year, interval):
        annual_para_count = 0
        start = year
        end = year + interval-1
        sittings = [fn for fn in z.filelist
             if fn.filename.startswith("dail/AK-dail")
             and start <= int(re.search("dail-(\d{4})-\d{2}-\d{2}.xml", fn.filename).group(1)) <=end]
        logging.info("Reading data for {} sittings between {} and {}".format(len(sittings), start, end))
        bz_file = "{}/paragraphs_{}-{}.bz2".format(output_dirpath, start, end)
        with BZ2File(bz_file, "w") as bz:
            logging.info("Opening {}".format(bz_file))
            for i, sitting in enumerate(sittings):
                root = etree.fromstring(z.open(sitting).read())
                date = root.find(".//{*}FRBRWork/{*}FRBRdate").attrib['date']
                paragraphs = root.findall(".//{*}speech/{*}p")
                l = len(paragraphs)
                annual_para_count += l
                cumulative_para_count += l
                for paragraph in paragraphs:
                    text = " ".join(paragraph.xpath(".//text()"))
                    #nlp method here
                    bz.write((text+"\n").encode("utf-8"))
                if i % 50 == 0:
                    logging.info("Written {} paragraphs from {} sittings for {}".format(annual_para_count, i, year))
                    logging.info("File position: {}, Cumulative paragraph count: {}".format(bz.tell(), cumulative_para_count))

            logging.info("Closing {}".format(bz_file))

@click.command()
@click.argument('start_year', default = 1922)
@click.argument('end_year', default = 2020)
@click.argument('interval', default = 10)
@click.argument('input_filepath', default = "../../data/external/AKN_dail.zip", type=click.Path(exists=True))
@click.argument('output_dirpath', default = "../../data/interim", type=click.Path(exists=True))
@click.argument('nlp', default = None)
def main(start_year, end_year, interval, nlp, input_filepath, output_dirpath):
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')
    extract_paragraphs(input_filepath, output_dirpath, start_year, end_year, interval, nlp)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
    logging.info('Project dir: {}'.format(project_dir))
    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    #load_dotenv(find_dotenv())

    main()

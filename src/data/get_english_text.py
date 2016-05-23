# -*- coding: utf-8 -*-
import os
import click
import logging
import spacy
import re
from zipfile import ZipFile
from lxml import etree
from dotenv import find_dotenv, load_dotenv
from datetime import datetime
from collections import defaultdict
from polyglot.text import Text as Poly
from nltk.corpus import stopwords
from concurrent.futures import ProcessPoolExecutor

NS = {"akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD13"}


class LanguagePipeline:
    def __init__(
            self, input_filepath, year
            ):

        self.input_filepath = input_filepath
        self.year = year
        self.z = ZipFile(self.input_filepath)
        self.sittings = [fn.filename for fn  in self.z.filelist if str(year) in fn.filename]

        self.uris = []

    def __iter__(self):
        for i, para in enumerate(self.extract_paragraphs()):
            if len(para)> 0:
                poly = " ".join(s.raw
                     for s in Poly(para).sentences if s.language.code=="en")
                logging.debug(poly)
                yield self.uris[i] + ": " + poly + "\n"

    def extract_paragraphs(self):
        '''Input is zipped Akoma Ntoso XML of type debateRecord.
        '''
        cumulative_para_count = 0
        self.uris = []
        logging.info("Year: {}, No. sittings: {}".format(self.year, len(self.sittings)))
        i = 0
        for i, sitting in enumerate(self.sittings):
            logging.debug(sitting)
            root = etree.fromstring(self.z.open(sitting).read())
            date = root.find(".//{*}FRBRWork/{*}FRBRdate").attrib['date']
            paragraphs = root.findall(".//{*}speech/{*}p")
            l = len(paragraphs)
            cumulative_para_count += l
            for para in paragraphs:
                uri = "{}/{}".format(date, para.attrib['eId'].replace("para_", ""))
                self.uris.append(uri)
                logging.debug("Date: {}, eId: {}".format(date, para.attrib['eId']))
                text = " ".join(para.xpath(".//text()"))
                yield text
            if i % 20 == 0:
                logging.info("Wrote {} paragraphs from {} sittings in {}".format(cumulative_para_count, i, self.year))
        logging.info(logging.info("Finished with {}: Wrote {} paragraphs from {} sittings".format(self.year, cumulative_para_count, i)))

def pipeline(input_filepath, output_dirpath, year):
    logging.info("Writing file for {}".format(year))
    pipe = LanguagePipeline(input_filepath, year)
    with open(os.path.join(output_dirpath, "english_{}.txt".format(year)), "w") as f:
        for line in pipe:
            f.write(line)
    logging.info("Closed file for {}".format(year))


@click.command()
@click.argument('start_year', default = 1922)
@click.argument('end_year', default = 2015)
@click.argument('input_filepath', default = "../../data/external/AKN_dail.zip", type=click.Path(exists=True))
@click.argument('output_dirpath', default = "../../data/interim/english", type=click.Path())
#@click.argument('nlp', default = None)
def main(input_filepath, output_dirpath, start_year, end_year):
    logger = logging.getLogger(__name__)
    logger.info('making English only interim data set from raw data')
    logging.info("Prepared pipeline")
    if not os.path.exists(output_dirpath):
        os.makedirs(output_dirpath)
    for year in range(start_year, end_year+1):
        with ProcessPoolExecutor(max_workers=4) as executor:
            r = executor.submit(pipeline, input_filepath, output_dirpath, year)
            logging.info("Result: {}".format(r.result()))
    logging.info("Finished")


if __name__ == '__main__':
    now = datetime.now()
    logfile = "logs/make-english-dataset_{}.log".format(now.strftime("%Y-%m-%dT%H-%M"))
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=logfile, level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
    logging.info('Project dir: {}'.format(project_dir))
    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    #load_dotenv(find_dotenv())

    main()

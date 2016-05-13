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
from datetime import datetime

NS = {"akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD13"}

class SpacyPipeline:
    def __init__(self, input_filepath, start_year, end_year):
        self.nlp = spacy.load("en")
        self.input_filepath = input_filepath
        self.start_year = start_year
        self.end_year = end_year
        self.z = ZipFile(self.input_filepath)
        self.sittings = self.sittings()


    def sittings(self):
        sittings = [fn for fn in self.z.filelist
                     if fn.filename.startswith("dail/AK-dail")
                     and self.start_year <= int(re.search("dail-(\d{4})-\d{2}-\d{2}.xml", fn.filename).group(1)) <=self.end_year]
        return sittings

    def __iter__(self):
        #indexing sentences by paragraph - will have to return to this and assign uris. hmm....

        for i, toks in enumerate(self.nlp.pipe(self.extract_paragraphs(), batch_size=100, n_threads=6)):
            #sentences = [[t.lemma_ for t in s if t.is_alpha and not t.is_stop] for s in toks.sents]

            sentences = [["{}_{}".format(tok.lemma_, tok.tag_) for tok in sent
                        if tok.is_alpha and not tok.is_stop and tok.tag_[0] in ("V", "N")]
                        for sent in toks.sents]
            for sentence in sentences:

                yield str(i)+": "+" ".join(sentence)+"\n"


    def paragraph_uris(self):
        paragraphs = []
        for sitting in self.sittings:
            root = etree.fromstring(self.z.open(sitting).read())
            date = root.find(".//{*}FRBRWork/{*}FRBRdate").attrib['date']
            paragraphs.extend("tagged/dail/{}/{}".format(date, p.attrib['eId']) for p in root.findall(".//{*}speech/{*}p"))

        uris = {str(i):uri for i, uri in enumerate(paragraphs)}
        return uris

    def extract_paragraphs(self):
        '''Input is zipped Akoma Ntoso XML of type debateRecord.
        Output is a compressed (BZ2) file for each period from start year to end year
        if interval is not set, the period is start_year to end_year-1
        For now, text is written as lemmas - swap out/ expand nlp section as need be
        '''
        cumulative_para_count = 0
        logging.info("Reading data for {} sittings between {} and {}".format(len(self.sittings), self.start_year, self.end_year))
        for i, sitting in enumerate(self.sittings):
            root = etree.fromstring(self.z.open(sitting).read())
            date = root.find(".//{*}FRBRWork/{*}FRBRdate").attrib['date']
            paragraphs = root.findall(".//{*}speech/{*}p")
            l = len(paragraphs)
            cumulative_para_count += l
            for para in paragraphs:
                logging.debug("Date: {}, eId: {}".format(date, para.attrib['eId']))
                #get over attribute error for 6 dots.
                text = " ".join(para.xpath(".//text()")).replace("..", " ")
                yield text
            if i % 20 == 0:
                logging.info("Written {} paragraphs from {} sittings".format(cumulative_para_count, i))


@click.command()
@click.argument('start_year', default = 1922)
@click.argument('end_year', default = 2016)
@click.argument('input_filepath', default = "../../data/external/AKN_dail.zip", type=click.Path(exists=True))
@click.argument('output_dirpath', default = "../../data/processed", type=click.Path(exists=True))
#@click.argument('nlp', default = None)
def main(start_year, end_year, input_filepath, output_dirpath):
    logger = logging.getLogger(__name__)
    logger.info('making tagged data set from raw data')
    pipe = SpacyPipeline(input_filepath, start_year, end_year)
    uris = pipe.paragraph_uris()
    logging.info("Indexed {} paragraphs".format(len(uris)))
    logging.info("Prepared spaCy pipeline")
    bz_file = "{}/tagged_{}_{}.bz2".format(output_dirpath, start_year, end_year)
    with BZ2File(bz_file, "w") as bz:
        logging.info("Opening {}".format(bz_file))
        for sentence in pipe:
            sentence = uris[sentence[0]]+sentence[1:]
            bz.write((sentence).encode("utf-8"))
        logging.info("Closing {}".format(bz_file))
    logging.info("Finished")


if __name__ == '__main__':
    now = datetime.now()
    logfile = "logs/tagging_{}.log".format(str(now).split(":")[0].replace(" ", "-"))
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=logfile, level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
    logging.info('Project dir: {}'.format(project_dir))
    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    #load_dotenv(find_dotenv())

    main()

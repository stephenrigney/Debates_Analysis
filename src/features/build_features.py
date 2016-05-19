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

NS = {"akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD13"}


class TextPipeline:
    def __init__(
            self, input_filepath,
            start_year, end_year,
            token_type="para"):
        self.format = token_type
        if self.format.startswith("spacy"):
            self.nlp = spacy.load("en")
        else:
            self.stopwords = set(stopwords.words("english"))
        self.input_filepath = input_filepath
        self.start_year = start_year
        self.end_year = end_year
        self.z = ZipFile(self.input_filepath)
        self.sittings = self.sittings()

        self.uris = []

    def __iter__(self):
        for year in sorted(self.sittings):
            yield "!!open " + year
            if self.format.startswith("spacy"):
                pipe = self.spacy_pipeline(year)

            else:
                pipe =self.polyglot_pipeline(year)
            for line in pipe:
                yield line
            yield "!!close " + year

    def polyglot_pipeline(self, year):
        for i, para in enumerate(self.extract_paragraphs(year)):
            para = para.replace("co-op", "coop")
            if len(para)> 0:
                poly = ".".join([" ".join(["{}/{}".format(w[0].lower(), w[1]) for w in s.pos_tags
                     if w[0] not in self.stopwords and
                     w[0].isalpha() and
                     w[1] in ["ADV", "ADJ", "VERB", "PROPN", "NOUN"]])
                     for s in Poly(para).sentences if s.language.code=="en"])
                logging.debug(poly)
                if len(poly) > 3:
                    yield self.uris[i] + ": " + poly + "\n"

    def spacy_pipeline(self, year):
        for i, toks in enumerate(
                self.nlp.pipe(self.extract_paragraphs(year),
                        batch_size=100, n_threads=6)):
            #sentences = [[t.lemma_ for t in s if t.is_alpha and not t.is_stop] for s in toks.sents]
            if self.format == "spacy-sent":
                for sent in (self.iter_sentences(toks, i)):
                    yield sent
            else:
                yield self.iter_paragraphs(toks, i)

    def sittings(self):
        sittings = defaultdict(list)
        for fn in self.z.filelist:
            if fn.filename.startswith("dail/AK-dail"):
                sitting_year = re.search("dail-(\d{4})-\d{2}-\d{2}.xml",
                                    fn.filename).group(1)
                if self.start_year <= int(sitting_year) <= self.end_year:
                    sittings[sitting_year].append(fn.filename)
        return sittings

    def iter_sentences(self, toks, i):
        sentences = [["{}/{}".format(tok.lemma_, tok.pos_) for tok in sent
                if tok.is_alpha and not
                tok.is_stop and
                tok.pos_ in ["ADV", "ADJ", "VERB", "PROPN", "NOUN"]]
                for sent in toks.sents]
        for sentence in sentences:
            yield str(i)+": "+" ".join(sentence)+"\n"

    def iter_paragraphs(self, toks, i):
        paragraph = ["{}_{}".format(tok.lemma_, tok.pos_) for tok in toks
                if tok.is_alpha and not
                tok.is_stop and
                tok.pos_ in ["ADV", "ADJ", "VERB", "PROPN", "NOUN"]]
        return self.uris[i] + ": " + " ".join(paragraph)+"\n"

    def extract_paragraphs(self, year):
        '''Input is zipped Akoma Ntoso XML of type debateRecord.
        '''
        cumulative_para_count = 0
        self.uris = []
        logging.info("Year: {}, No. sittings: {}".format(year, len(self.sittings[year])))
        for i, sitting in enumerate(self.sittings[year]):
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
                logging.info("Wrote {} paragraphs from {} sittings in {}".format(cumulative_para_count, i, year))
        logging.info(logging.info("Finished with {}: Wrote {} paragraphs from {} sittings".format(year, cumulative_para_count, i)))


@click.command()
@click.argument('start_year', default = 1922)
@click.argument('end_year', default = 2016)
@click.argument('input_filepath', default = "../../data/external/AKN_dail.zip", type=click.Path(exists=True))
@click.argument('output_dirpath', default = "../../data/processed", type=click.Path(exists=True))
#@click.argument('nlp', default = None)
def main(input_filepath, output_dirpath, start_year, end_year):
    method = "poly-pos-para"
    logger = logging.getLogger(__name__)
    logger.info('making tagged data set from raw data')
    pipe = TextPipeline(input_filepath, start_year, end_year)

    logging.info("Prepared spaCy pipeline")
    directory = "{}/{}_{}-{}".format(output_dirpath, method, start_year, end_year)
    if not os.path.exists(directory):
        os.makedirs(directory)
    for sentence in pipe:
        if sentence.startswith("!!open"):
            fn = "{}_{}.txt".format(method, sentence.split()[-1])
            f = open(os.path.join(directory, fn), "w")
            logging.info("Writing file for {}".format(fn))
        elif sentence.startswith("!!close"):
            f.close()
            logging.info("Closed file for {}".format(sentence.split()[-1]))
        else:
            sentence = sentence
            f.write((sentence))
    logging.info("Finished")


if __name__ == '__main__':
    now = datetime.now()
    logfile = "logs/make-dataset_{}.log".format(now.strftime("%Y-%m-%dT%H-%M"))
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=logfile, level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
    logging.info('Project dir: {}'.format(project_dir))
    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    #load_dotenv(find_dotenv())

    main()

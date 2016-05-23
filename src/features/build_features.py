# -*- coding: utf-8 -*-
import os
import click
import logging
import spacy
import tarfile
import multiprocessing
from dotenv import find_dotenv, load_dotenv
from datetime import datetime
from collections import defaultdict



class TextPipeline:
    def __init__(
            self, input_filepath,
            start_year, end_year,
            token_type="para"):
        self.format = token_type
        self.nlp = spacy.load("en")
        self.input_filepath = input_filepath
        self.start_year = start_year
        self.end_year = end_year

        self.uris = []

    def __iter__(self):
        for year in range(self.start_year, self.end_year+1):
            yield "!!open " + str(year)
            pipe = self.spacy_pipeline(year)
            for line in pipe:
                yield line
            yield "!!close " + str(year)

    def spacy_pipeline(self, year):
        for i, toks in enumerate(
                self.nlp.pipe(self.extract_paragraphs(year),
                        batch_size=200, n_threads=cores)):
            if self.format == "sent":
                for sent in (self.iter_sentences(toks, i)):
                    yield sent
            else:
                yield self.iter_paragraphs(toks, i)


    def iter_sentences(self, toks, i):
        sentences = [["{}/{}".format(tok.lemma_, tok.pos_) for tok in sent
                if tok.is_alpha and not
                tok.is_stop and
                tok.pos_ in ["ADV", "ADJ", "VERB", "PROPN", "NOUN"]]
                for sent in toks.sents]
        for sentence in sentences:
            yield str(i)+": "+" ".join(sentence)+"\n"

    def iter_paragraphs(self, toks, i):
        paragraph = ["{}/{}".format(tok.lemma_, tok.pos_) for tok in toks
                if tok.is_alpha and not
                tok.is_stop and
                tok.pos_ in ["ADV", "ADJ", "VERB", "PROPN", "NOUN"]]
        return self.uris[i] + ": " + " ".join(paragraph)+"\n"

    def extract_paragraphs(self, year):
        self.uris = []
        i = []
        logging.info("Year: {}".format(year))
        with tarfile.open(self.input_filepath, encoding="utf-8") as tf:
            filenames = [fn for fn in tf if str(year) in fn.name]
            for fn in filenames:
                fileobj = tf.extractfile(fn)
                for i, line in enumerate(fileobj):
                    uri_text = line.decode("utf-8").split(": ")
                    self.uris.append(uri_text[0])
                    yield uri_text[1]
                    if i % 2000 == 0:
                        logging.info("Wrote {}, of {} paragraphs for {}".format(uri_text[0], i, year))
        logging.info(logging.info("Finished with {}: Wrote {} paragraphs".format(year, i)))



@click.command()
@click.argument('start_year', default = 1923)
@click.argument('end_year', default = 2015)
@click.argument('input_filepath', default = "../../data/interim/english-dail_1922-2015.tar.gz", type=click.Path(exists=True))
@click.argument('output_dirpath', default = "../../data/processed", type=click.Path(exists=True))
#@click.argument('nlp', default = None)
def main(input_filepath, output_dirpath, start_year, end_year):
    method = "spacy-para-lemma-tag"
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
    cores = multiprocessing.cpu_count()

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
    logging.info('Project dir: {}'.format(project_dir))
    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    #load_dotenv(find_dotenv())

    main()

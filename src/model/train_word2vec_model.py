# -*- coding: utf-8 -*-
import os
import re
import click
import logging
import tarfile
import multiprocessing
from dotenv import find_dotenv, load_dotenv
from gensim.models import Word2Vec, Doc2Vec
from gensim.models.doc2vec import LabeledSentence
from datetime import datetime


class ExtractLines:
    def __init__(self, dirname, start_year, end_year, model):
        self.dirname = dirname
        self.start_year = start_year
        self.end_year = end_year
        self.model = model

    def __iter__(self):
        with tarfile.open(self.dirname, encoding="utf-8") as tf:
            for fn in tf:
                sitting_year = re.search("_(\d{4}).txt",
                                    fn.name)
                if fn.name.endswith("txt") and self.start_year <= int(sitting_year.group(1)) < self.end_year:
                    fileobj = tf.extractfile(fn)
                    for line in fileobj:
                        line = line.decode("utf-8").split()
                        if self.model == "word2vec":
                            yield line[1:]
                        else:
                            yield LabeledSentence(words=line[1:], tags=[line[0][:-1]])



@click.command()
@click.argument('start_year', default = 1920)
@click.argument('end_year', default = 2001)
@click.argument('interval', default=5)
@click.argument('model', default="word2vec")
@click.argument('input_filepath', default="../../data/processed/poly-pos-para_1922-2016.tar.gz", type=click.Path(exists=True))
@click.argument('output_filepath', default="../../models/word2vec", type=click.Path())
def main(input_filepath, output_filepath, start_year, end_year, interval, model):
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')
    if not os.path.exists(output_filepath):
        os.makedirs(output_filepath)
    logging.info("Start year: {}, End year: {}, Interval: {} years".format(start_year, end_year, interval))
    for decade in range(start_year, end_year, interval):
        logging.info("Training model for period from {} to {}".format(decade, decade+19))
        lines = ExtractLines(input_filepath, decade, decade+19, model)
        for model in [(Word2Vec, "word"), (Doc2Vec, "doc")]:
            m = model[0](lines, workers=cores)
            name = model[1]
            fn = input_filepath.split("/")[-1].split(".")[0]
            filename = "{}-spacy-pos-lemma_{}-{}.{}2v".format(name, fn, decade, decade+19, name[1])
            m.save(os.path.join(output_filepath, filename))
            logging.info("Saved: {}".format(filename))
    logging.info("Finished")



if __name__ == '__main__':
    now = datetime.now()
    logfile = "logs/train_word2vec_model_{}.log".format(now.strftime("%Y-%m-%dT%H-%M"))
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=logfile, level=logging.INFO, format=log_fmt)
    cores = multiprocessing.cpu_count()
    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()

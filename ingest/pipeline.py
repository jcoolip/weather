import logging
from datetime import datetime

import adzuna
import industry_classify
import Jobicy
import jooble
import jsearch
import remotive
import remotive_skills
import usajobs
import usajobs_skills

now = datetime.now()

logging.basicConfig(
    level=logging.DEBUG,
    filename="pipeline.log",
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def main():
    logging.info("-- BEGIN --")
    print("start")
    remotive.main()
    remotive_skills.main()
    usajobs.main()
    usajobs_skills.main()
    adzuna.main()
    jsearch.main()
    jooble.main()
    Jobicy.main()
    industry_classify.main()
    print("finish")
    logging.info("-- COMPLETE --")


if __name__ == "__main__":
    main()

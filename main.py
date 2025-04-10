import time

from data_analyse import AnalyseData


if __name__ == "__main__":

    analyser = AnalyseData()
    while True:
        analyser.analyse()
        time.sleep(1)


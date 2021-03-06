import sys
from main.job.pipeline import PySparkJob


input_path = sys.argv[1]


def main():
    job = PySparkJob()

    # Load input data to DataFrame
    print("<<Reading>>")
    vaccines = job.read_csv(sys.argv[1])

    # Get number of available vaccines
    print("<<Distinct Vaccines>>")
    nb_vaccines = job.count_available_vaccines(vaccines)
    print(nb_vaccines)

    # Get name of earliest used vaccine
    print("<<Earliest Used Vaccine>>")
    earliest_vaccine = job.find_earliest_used_vaccine(vaccines)
    print(earliest_vaccine)

    # Get total_vaccinations by country
    print("<<Total Vaccinations By Country>>")
    total_vaccinations = job.total_vaccinations_per_country(vaccines)
    total_vaccinations.show()

    job.stop()


if __name__ == '__main__':
    main()

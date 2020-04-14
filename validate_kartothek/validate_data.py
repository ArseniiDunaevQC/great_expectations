import os
from datetime import datetime
from enum import Enum

import great_expectations as ge


def enumerate_options(options):
    """
    Pretty printing of the options for subsequent selection by user.

    :param options: Options to be chosen from.
    :return: String with one option per line, numbered from 1 to amount of options.
    """
    output = ""
    for index, option in enumerate(options):
        output += "    " + str(index + 1) + ". " + repr(option).strip("'") + "\n"
    return output + ": "


def get_valid_input_from_user(inquiry, options):
    """
    Method for asking user to provide a valid input.

    :param inquiry: Question to be asked.
    :param options: List of available options.
    :return: Answer provided by the user. This answer is forced to be one of the available options.
    """
    user_answer = None

    while True:
        chosen_option = input(inquiry + enumerate_options(options))
        if chosen_option.isnumeric():
            try:
                user_answer = options[int(chosen_option) - 1]
                break
            except IndexError:
                print("There is no option under the given index. Choose an available index/option.")
                continue
        else:
            user_answer = chosen_option  # NOTE: does not work for Enums
            if user_answer not in options:
                print("There is no such option. Choose an available index/option.")
                continue
            break

    return user_answer


def user_input():
    """
    This method asks user to provide an input, and captures the given information.

    :return: 4-tuple:
                mechanism: Chosen :class:`ValidationMechanism`;
                expectation_suite: Name of the chosen expectation suite, towards which data will be validated;
                datasource: Name of the GE datasource;
                base_path: Path to the data (namely, to the whole dataset).
    """
    # 2) get the validation mechanism
    mechanisms = []
    for Mechanism in ValidationMechanism:
        mechanisms.append(Mechanism)
    mechanism = get_valid_input_from_user("How would you like your dataset to be validated?\n", mechanisms)

    # 3) list expectation suites and choose one
    expectation_suites = context.list_expectation_suite_names()
    expectation_suite = get_valid_input_from_user("Which expectation suite should be used for validation?\n", expectation_suites)

    # 4) list datasources of the type SparkDFDatasource available in project and choose one
    datasources = [datasource['name'] for datasource in context.list_datasources() if datasource['class_name'] == 'SparkDFDatasource']
    datasource = get_valid_input_from_user("Which datasource do you want to validate?\n", datasources)

    # 5) list spark datasets available for validation and choose one
    datasets = [data_folder for data_folder in os.listdir("data/") if os.path.isdir("data/" + data_folder)]
    path = get_valid_input_from_user("Which dataset do you want to validate?\n", datasets)
    base_path = "../data/" + path + "/"

    # 6) list tables in the dataset and choose one
    tables = os.listdir("data/" + path + "/")
    if len(tables) == 1:
        base_path += tables[0] + "/"
    else:
        table = get_valid_input_from_user("Which table do you want to validate?\n", tables)
        base_path += table + "/"

    return mechanism, expectation_suite, datasource, base_path


def get_batch_from_files():
    """
    Method for getting a list of GE data batches to be validated.
    This method generates a batch from each ".parquet" file of the dataset by listing all available partitions
    in all data assets (partitions should be files, not directories) through generator.

    NOTE: Does not work for deeply (more than one subdirectory) nested datasets.

    :return: List of batches to be validated. Each batch corresponds to one ".parquet" file.
    """
    batches_to_validate = []

    # 6) list all available data assets
    data_assets = context.get_available_data_asset_names()[datasource_name]['subdir_reader']['names']
    for data_asset in data_assets:
        # 6.1) handle data asset name as directory
        if data_asset[1] == 'directory':
            data_asset_name = data_asset[0] + "/"

        # 6.2) handle data asset name as file (single 'partition')
        elif data_asset[1] == 'file':
            data_asset_name = data_asset[0]

        # 7) list partitions and get a batch of data for each partition in the data asset
        partitions = subdir_reader_generator.get_available_partition_ids(data_asset_name)
        for partition in partitions:
            batch_kwargs = context.build_batch_kwargs(datasource_name, "subdir_reader", data_asset_name,
                                                      partition_id=partition)
            batch_kwargs['reader_method'] = "parquet"
            batch = context.get_batch(batch_kwargs, expectation_suite_name)
            batches_to_validate.append(batch)

    return batches_to_validate


def get_batch_from_partitions():
    """
    Method for getting a list of GE data batches to be validated.
    This method generates a batch from each data asset (partition folder 'column?value') of the dataset by listing all
    available data assets through generator.

    NOTE: Does not work for deeply (more than one subdirectory) nested datasets.

    :return: List of batches to be validated. Each batch corresponds to one 'column?value' partition folder.
    """
    batches_to_validate = []

    # 6) list all available data assets
    data_assets = context.get_available_data_asset_names()[datasource_name]['subdir_reader']['names']
    for data_asset in data_assets:
        # 6.1) handle data asset name as directory
        if data_asset[1] == 'directory':
            data_asset_name = data_asset[0] + "/"

        # 6.2) handle data asset name as file (single 'partition')
        elif data_asset[1] == 'file':
            data_asset_name = data_asset[0]

        # 7) get a batch of data for the whole data asset
        batch_kwargs = {'path': subdir_reader_generator.base_directory + data_asset_name, 'datasource': datasource_name,
                        'reader_method': "parquet"}
        batch = context.get_batch(batch_kwargs, expectation_suite_name)
        batches_to_validate.append(batch)

    return batches_to_validate


def get_batch_from_dataset():
    """
    Method for getting a list of GE data batches to be validated.
    This method generates a batch from the whole dataset by listing all available partitions in all data assets
    (partitions should be ".parquet" files, not directories) through generator, and reading them (from their paths)
    with spark in one dataframe.

    NOTE: Does not work for deeply (more than one subdirectory) nested datasets.

    :return: List of batches to be validated. Includes only one batch corresponding to the whole dataset/table (if multiple tables).
    """
    partition_paths = []

    # 6) list all available data assets
    data_assets = context.get_available_data_asset_names()[datasource_name]['subdir_reader']['names']
    for data_asset in data_assets:
        # 6.1) handle data asset name as directory
        if data_asset[1] == 'directory':
            data_asset_name = data_asset[0] + "/"

        # 6.2) handle data asset name as file (single 'partition')
        elif data_asset[1] == 'file':
            data_asset_name = data_asset[0]

        # 7.1) get paths of all ".parquet" files
        partitions = subdir_reader_generator.get_available_partition_ids(data_asset_name)
        for partition in partitions:
            batch_kwargs = context.build_batch_kwargs(datasource_name, "subdir_reader", data_asset_name,
                                                      partition_id=partition)
            partition_paths.append(batch_kwargs['path'])

    # 7.2) read all ".parquet" files as one spark dataframe, and get it as batch of data
    df = datasource.spark.read.parquet(*partition_paths)
    batch_kwargs = {'dataset': df, 'datasource': datasource_name}
    batch = context.get_batch(batch_kwargs, expectation_suite_name)
    batch.batch_kwargs['path'] = subdir_reader_generator.base_directory  # add path to distinguish between validations

    return [batch]


def get_batch_from_nested_dataset():
    """
    Method for getting a list of GE data batches to be validated.
    This method generates a batch from the whole dataset by recursively listing all available ".parquet" files in all
    data assets through generator (until no unvisited directories left), and reading them (from their paths)
    with spark in one dataframe.

    NOTE: To read all ".parquet" files at once, the method uses already created SparkSession in 'datasource.spark',
    which is public now, but can be changed in later GE versions and thus be impossible to use.

    :return: List of batches to be validated. Includes only one batch corresponding to the whole dataset/table (if multiple tables).
    """
    partition_paths = []

    # 6) while there are available data assets, go through recursively:
    data_assets = context.get_available_data_asset_names()[datasource_name]['subdir_reader']['names']
    while data_assets:
        for data_asset in data_assets:
            # 6.1) list available partitions in directories, and add them to data assets
            if data_asset[1] == 'directory':
                data_assets += [(data_asset[0] + "/" + partition_id, 'directory')
                                if os.path.isdir(subdir_reader_generator.base_directory + data_asset[0] + "/" + partition_id)
                                else (data_asset[0] + "/" + partition_id, 'file')
                                for partition_id in
                                subdir_reader_generator.get_available_partition_ids(data_asset[0])]

            # 6.2) add paths to ".parquet" files (single partitions) to 'partition_paths'
            elif data_asset[1] == 'file':
                partition_paths.append(subdir_reader_generator.base_directory + data_asset[0] + ".parquet")

            data_assets.remove(data_asset)

    # 7) read all ".parquet" files as one spark dataframe, and get it as batch of data
    df = datasource.spark.read.parquet(*partition_paths)
    batch_kwargs = {'dataset': df, 'datasource': datasource_name}
    batch = context.get_batch(batch_kwargs, expectation_suite_name)
    batch.batch_kwargs['path'] = subdir_reader_generator.base_directory  # add path to distinguish between validations

    return [batch]


def get_batch_from_nested_dataset_using_wildcard():
    """
    Method for getting a list of GE data batches to be validated.
    This method generates a batch from the whole dataset by using characteristics of kartothek, namely, the uniform
    data structure (i.e. consistent directory depth for each dataset) and of spark (wildcard symbol '*').
    Therefore, spark will find all ".parquet" files ("*.parquet") in all subdirectories ("*/") of the dataset.

    The method delivers the same output as 'get_batch_from_nested_dataset()', but it is more conform with GE
    architecture, and thus is the most preferable option.

    :return: List of batches to be validated. Includes only one batch corresponding to the whole dataset/table (if multiple tables).
    """
    # 6) find the maximal directory depth
    max_depth = 0
    for root, dirs, files in os.walk(subdir_reader_generator.base_directory):
        if files:  # not empty
            for file in files:
                path = os.path.relpath(os.path.join(root, file), subdir_reader_generator.base_directory)

                depth = path.count("/")
                if depth > max_depth:
                    max_depth = depth

    path_mask = "*/" * max_depth + "*.parquet"

    # 7) get a batch of data using the path mask
    batch_kwargs = {'path': subdir_reader_generator.base_directory + path_mask, 'datasource': datasource_name,
                    'reader_method': "parquet"}
    batch = context.get_batch(batch_kwargs, expectation_suite_name)

    return [batch]


class ValidationMechanism(Enum):
    file = ("validate separate files (separate Validation Result for each '.parquet' file)",
            get_batch_from_files)
    partition = ("validate partitions (separate Validation Result for each partition, e.g. 'column?value')",
                 get_batch_from_partitions)
    dataset = ("validate dataset (one Validation Result for the whole dataset/table)",
               get_batch_from_dataset)
    nested_dataset = ("validate dataset (works also for deeply nested dataset directories)",
                      get_batch_from_nested_dataset)
    nested_dataset_wildcard = ("validate dataset (works also for deeply nested dataset directories, "
                               "but is more conform with GE architecture)",
                               get_batch_from_nested_dataset_using_wildcard)

    def __repr__(self):
        return self.name.replace("_", " ") + ": " + self.value[0]


if __name__ == "__main__":
    # 1) load data context of GE
    context = ge.data_context.DataContext()

    # 2-4) get input from user
    validation_mechanism, expectation_suite_name, datasource_name, base_directory = user_input()

    # 5) add a temporal SubdirReader batch kwargs generator to generate data assets
    context.add_generator(datasource_name, "subdir_reader", "SubdirReaderBatchKwargsGenerator", base_directory=base_directory)
    datasource = context.get_datasource(datasource_name=datasource_name)
    subdir_reader_generator = datasource.get_generator(generator_name="subdir_reader")

    # 6-7) get batches of data to validate
    batches_to_validate = validation_mechanism.value[1]()

    # 8) validate a bunch of data assets using Validation Operator
    run_id = datetime.now().isoformat().replace(":", "").replace("-", "") + "Z"
    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=batches_to_validate,
        run_id=run_id)

    # 9) show Data Docs
    context.open_data_docs()

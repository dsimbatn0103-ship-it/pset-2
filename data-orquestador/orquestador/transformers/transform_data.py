if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    #columnas_limpias = []

    #for columna in data.columns:
    #    columnas_limpias.append(columna.lower())

    data.columns = [columna.lower() for columna in data.columns]

    data.rename(columns={
        #<columna original>:<nuevo nombre>
        'vendorid': 'vendor_id',
        'ratecodeid': 'rate_code_id',
        'pulocationid': 'pu_location_id',
        'dolocationid': 'do_location_id'
    }, inplace = True)

    #data['passenger_count'] = data['passenger_count'].astype('Int64')
    #data['rate_code_id'] = data['rate_code_id'].astype('Int64')

    #data.fillna(-1, inplace=True)

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

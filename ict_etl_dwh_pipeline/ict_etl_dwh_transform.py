if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
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
    # Specify your transformation logic here

    # reset the index
    df = df.drop_duplicates().reset_index(drop=True)

    # make primary key for fact table
    df['ict_id'] = df.index

    '''''''''''''''''''''''''''''''''''''''''
    1. dim_date
    '''''''''''''''''''''''''''''''''''''''''
    # reset the index
    dim_date = df[['start_year', 'end_year']].reset_index(drop=True)
    # surrogate key for passenger_count_dim
    dim_date['date_id'] = dim_date.index
    # rearrange the column
    dim_date = dim_date[['date_id','start_year', 'end_year']]


    '''''''''''''''''''''''''''''''''''''''''
    2. dim_sex
    '''''''''''''''''''''''''''''''''''''''''
    # reset the index
    dim_sex = df[['sex']].reset_index(drop=True)
    # surrogate key for passenger_count_dim
    dim_sex['sex_id'] = dim_sex.index
    # rearrange the column
    dim_sex = dim_sex[['sex_id','sex']]


    '''''''''''''''''''''''''''''''''''''''''
    3. dim_source
    '''''''''''''''''''''''''''''''''''''''''
    # reset the index
    dim_source = df[['source']].reset_index(drop=True)
        
    # surrogate key for passenger_count_dim
    dim_source['source_id'] = dim_source.index

    # rearrange the column
    dim_source = dim_source[['source_id','source']]


    '''''''''''''''''''''''''''''''''''''''''
    4. dim_country
    '''''''''''''''''''''''''''''''''''''''''
    # reset the index
    dim_country = df[['country']].reset_index(drop=True)
    # surrogate key for passenger_count_dim
    dim_country['country_id'] = dim_country.index
    # rearrange the column
    dim_country = dim_country[['country_id','country']]


    '''''''''''''''''''''''''''''''''''''''''
    5. dim_ISO3
    '''''''''''''''''''''''''''''''''''''''''
    # reset the index
    dim_ISO3 = df[['ISO3']].reset_index(drop=True)
    # surrogate key for passenger_count_dim
    dim_ISO3['ISO3_id'] = dim_ISO3.index
    # rearrange the column
    dim_ISO3 = dim_ISO3[['ISO3_id','ISO3']]


    '''''''''''''''''''''''''''''''''''''''''
    6. dim_region
    '''''''''''''''''''''''''''''''''''''''''
    region_names = {
        'EAP': 'East Asia and the Pacific',
        'ECA': 'Europe and Central Asia',
        'EECA': 'Eastern Europe and Central Asia',
        'ESA': 'Eastern and Southern Africa',
        'LAC': 'Latin America and the Caribbean',
        'MENA': 'Middle East and North Africa',
        'NA': 'North America',
        'SA': 'South Asia',
        'SSA': 'Sub-Saharan Africa',
        'WCA': 'West and Central Africa',
    }

    # reset the index
    dim_region = df[['region_code']].reset_index(drop=True)
    # surrogate key for passenger_count_dim
    dim_region['region_id'] = dim_region.index
    # mapping region code to region name
    dim_region['region_name'] = dim_region['region_code'].map(region_names)
    # rearrange the column
    dim_region = dim_region[['region_id','region_code', 'region_name']]


    '''''''''''''''''''''''''''''''''''''''''
    7. fact_table_ict
    '''''''''''''''''''''''''''''''''''''''''
    # merge dim table to get primary key from each dim table into foreign key in fact table
    fact_table = df.merge(dim_sex, left_on='ict_id', right_on='sex_id') \
                .merge(dim_date, left_on='ict_id', right_on='date_id') \
                .merge(dim_source, left_on='ict_id', right_on='source_id') \
                .merge(dim_country, left_on='ict_id', right_on='country_id') \
                .merge(dim_ISO3, left_on='ict_id', right_on='ISO3_id') \
                .merge(dim_region, left_on='ict_id', right_on='region_id') 

    # rearrange the column    
    fact_table = fact_table[['ict_id', 'sex_id', 'date_id', 'source_id', 'country_id', 'region_id',
                            'criteria_1', 'criteria_2', 'criteria_3', 'criteria_4', 'criteria_5',
                            'criteria_6', 'criteria_7', 'criteria_8', 'criteria_9', 'criteria_10']]
    


    return {'dim_sex': dim_sex.to_dict(orient='dict'),
            'dim_date': dim_date.to_dict(orient='dict'),
            'dim_source': dim_source.to_dict(orient='dict'),
            'dim_country': dim_country.to_dict(orient='dict'),
            'dim_ISO3': dim_ISO3.to_dict(orient='dict'),
            'dim_region': dim_region.to_dict(orient='dict'),
            'fact_table': fact_table.to_dict(orient='dict')
     }


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

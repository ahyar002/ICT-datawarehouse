import pandas as pd

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
    # Rename 10 criteria for more readable, clear, and concise
    # Create a dictionary to map old column names to new column names
    column_mapping = {
        'UNICEF Region': 'region_code',
        'Sex': 'sex',
        'Source': 'source',
        'Copied or moved a file or folder': 'criteria_1',
        'Used a copy and paste tool to duplicate or move information within a document': 'criteria_2',
        'Sent e-mail with attached file, such as a document, picture or video': 'criteria_3',
        'Used a basic arithmetic formula in a spreadsheet': 'criteria_4',
        'Connected and installed a new device, such as a modem, camera or printer': 'criteria_5',
        'Found, downloaded, installed and configured software': 'criteria_6',
        'Created an electronic presentation with presentation software, including text, images, sound, video or charts': 'criteria_7',
        'Transferred a file between a computer and other device': 'criteria_8',
        'Wrote a computer program in any programming language': 'criteria_9',
        'Performed at least one out of nine activities': 'criteria_10',
    }

    # Use the rename method to rename the columns
    df.rename(columns=column_mapping, inplace=True)


    # Define a function to split the 'Year' values
    def split_year(year_value):
        parts = year_value.split('-')
        if len(parts) == 2:
            return parts[0], f'20{parts[1]}'
        else:
            return year_value, year_value

    # Apply the split_year function to create 'Start Year' and 'End Year' columns
    df[['start_year', 'end_year']] = df['Year'].apply(split_year).apply(pd.Series).astype(int)

    # Drop the original 'Year' 
    df.drop('Year', axis=1, inplace=True)



    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

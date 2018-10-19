
def tocsv(dataset, outcome, output):
    """
    This function takes the files as input and output a well formed csv file.
    """
    # Import and read dataset.
    import os
    files = os.listdir(dataset + "/")

    data = []
    for filename in files:
        with open(os.path.join(dataset, filename), 'r') as f:
            data.append(f.read())

    # Import and read outcome file.
    with open(outcome, 'r') as o:
        out = o.read()

    # Pre-processing the outcome data and make them as a well-structured dataframe.
    out = out.split()[1:]

    for i in range(len(out)):
        out[i] = out[i].split(',')

    import pandas as pd
    for i in range(len(out)):
        for j in range(len(out[i])):
            out[i][j] = float(out[i][j])
    df_out = pd.DataFrame(out, columns = ['RecordID','SAPS-I','SOFA','Length_of_stay','Survival','In-hospital_death'])

    
    # Pre-processing the dataset.
    for i in range(len(data)):
        data[i] = data[i].split()[1:]

    var = []
    for i in range(len(data)):
        if len(var) < 42:
            for j in range(len(data[i])):
                # Take variable names and ignore if it is missing.
                if (data[i][j].split(',')[1] not in var) and (data[i][j].split(',')[1] != ''):
                    var.append(data[i][j].split(',')[1])
        else:
            break

    # Create new time series variables.
    mmm = ['12_min','12_max','12_mean','36_min','36_max','36_mean']
    result = ['{}_{}'.format(a,b) for b in mmm for a in var[5:]]
    result.sort()
    result = var[:5] + result

    import pandas as pd
    df = pd.DataFrame(columns = result)

    # Manipulate dataset to be a data frame.
    for i in range(len(data)):
        values = set(map(lambda x:x.split(',')[1], data[i]))
        # Handle the case that variable names are missing.
        if '' in values:
            values.remove('')
        firstdict ={ y + '_12' : [float(z.split(',')[2]) for z in data[i] if z.split(',')[1] == y and z.split(',')[0] < '12:00'] for y in values}
        seconddict ={ y + '_36' : [float(z.split(',')[2]) for z in data[i] if z.split(',')[1] == y and z.split(',')[0] >= '12:00'] for y in values}
        totaldict = dict(list(firstdict.items()) + list(seconddict.items()))
        new_dict = {l:v for l,v in totaldict.items() if v}
        totaldict.clear()
        totaldict.update(new_dict)
        aggdict = {}
        for k in totaldict:
            if (k == 'Age_12') or (k == 'RecordID_12') or (k == 'Gender_12') or (k == 'Height_12') or (k == 'ICUType_12'):
                aggdict[k[:-3]] = totaldict[k]
            else:
                aggdict[k + '_min'] = min(totaldict[k])
                aggdict[k + '_max'] = max(totaldict[k])
                aggdict[k + '_mean'] = sum(totaldict[k])/len(totaldict[k])
        df_temp = pd.DataFrame(aggdict)
        df = df.append(df_temp)

    # Merge dataset and outcome together.
    merge = pd.merge(df,df_out, on = ['RecordID'])

    # Output as csv file.
    output_name = output + '.csv'
    merge.to_csv(output_name,sep=',')




if __name__ == "__main__":
    dataset = input('Enter the folder name containing the dataset: ')
    outcome = input('Enter the outcome file name(with suffix): ')
    output = input('Enter the file name of the output csv file: ')
    tocsv(dataset, outcome, output)

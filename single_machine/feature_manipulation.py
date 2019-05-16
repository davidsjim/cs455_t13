def charToColumns(char, columnDefs):
    column=[]
    for element in columnDefs:
        subtree=char
        nodeIndex=0
        while nodeIndex < len(element['nodes']) :
            try:
                subtree=subtree[element['nodes'][nodeIndex]]
            except:
                break
            nodeIndex+=1
        value=None
        if nodeIndex >= len(element['nodes']):
            parsefn=element['parse'] if 'parse' in element else lambda x: x
            value=parsefn(subtree)
        else:
            value=0
        column.append(value)
    return column

def columnsToChar(cols, columnDefs):
    char={}
    for i in range(len(columnDefs)):
        column=columnDefs[i]
        label=column['label'] if 'label' in column else column['nodes'][-1]
        val=cols[i]
        char[label]=val
    return char

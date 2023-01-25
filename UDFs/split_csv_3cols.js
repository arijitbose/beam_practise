function transform(line) {
    var values = line.split(',');
    var obj = new Object();
    obj.col1 = values[0];
    obj.col2 = values[1];
    obj.col3 = values[2];
    var jsonString = JSON.stringify(obj);
    return jsonString;
}
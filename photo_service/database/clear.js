const mongoose = require('mongoose');

//------------------------------------------
const Schema = mongoose.Schema;
const binaryDataSchema = new Schema({
  photo_id: Number,
  binary_data: Buffer,
  photo_type: String,
});
//------------------------------------------
let BinaryDataModel = mongoose.model('binarydata', binaryDataSchema);

const DeleteAll = function () {

  mongoose.connect('mongodb://localhost/photoservice', () => {
    BinaryDataModel.remove({}).exec().then(result => {
      console.log('removed BinaryDataModel ', result.result);
    }).then(mongoose.connection.close());
  });

};

DeleteAll();

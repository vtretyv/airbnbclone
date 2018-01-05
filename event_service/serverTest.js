//jshint esversion:6
let request = require('supertest');
let server = require('./server/index.js');
let Promise = require('bluebird');
Promise.promisifyAll(server);

// ======================================================================
//                   Server Tests
// ======================================================================

describe('Server Handles Requests', function () {

  // increases time allowed for tests to run 
  this.timeout(15000);
  
  // checks for homepage to have 'Kick It' as title
  it('loads Homepage', (done) => {
    request(server)
    .get('/')
    .expect(200)
  });


// checks all other requests
  it('404 all other requests', (done) => {
    request(server)
      .get('/foo/bar')
      .expect(404, done);
  });
  
});
















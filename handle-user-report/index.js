const functions = require('@google-cloud/functions-framework');
const Firestore = require("@google-cloud/firestore");

const PROJECTID = "nasa-sparc";
const COLLECTION_NAME = "reports";

// Create a new client 
const firestore = new Firestore({
  projectId: PROJECTID,
  timestampsInSnapshots: true
});

functions.http('handleUserReport', async (req, res) => {
  if (req.method == "POST") {
    const { report, user_location, timestamp } = req.body;
    const { heard_wildfire, air_quality, smoke_intensity, smoke_description } = report;
    const { latitude, longitude } = user_location;

    return firestore.collection(COLLECTION_NAME).add({
      report: {
        heard_wildfire,
        air_quality,
        smoke_intensity,
        smoke_description
      },
      user_location: {
        latitude,
        longitude
      },
      timestamp
    })
      .then(doc => {
        return res.status(200).json({
          status: "success",
          message: `Stored new doc id ${doc.id}`,
          doc: req.body
        });
      })
      .catch(err => {
        console.error(err);
        return res.status(400).send({
          error: "Unable to store data.",
          err
        })
      })
  }
});

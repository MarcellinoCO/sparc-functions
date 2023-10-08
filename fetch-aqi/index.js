const functions = require('@google-cloud/functions-framework');

/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} res HTTP response context.
 */
functions.http('fetchAQIData', async (req, res) => {
  res.set("Access-Control-Allow-Origin", "*");

  if (req.method === "OPTIONS") {
    /* handle preflight OPTIONS request */
    
    res.set("Access-Control-Allow-Methods", "GET, POST");
    res.set("Access-Control-Allow-Headers", "Content-Type");

    // cache preflight response for 3600 sec
    res.set("Access-Control-Max-Age", "3600");
    
    return res.sendStatus(204);
  }

  try {
    const url = `https://airquality.googleapis.com/v1/currentConditions:lookup?key=${process.env.GMP_API_KEY}`;
    
    const { latitude, longitude, languageCode="en" } = req.body;

    const payload = {
      "universal_aqi": false,
      "location": {
          "latitude": latitude,
          "longitude": longitude
      },
      "extra_computations": [
          "HEALTH_RECOMMENDATIONS",
          "DOMINANT_POLLUTANT_CONCENTRATION",
          "POLLUTANT_CONCENTRATION",
          "LOCAL_AQI",
          "POLLUTANT_ADDITIONAL_INFO"
      ],
      "languageCode": languageCode
  }

    const options = {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload)
    }

    const response = await fetch(url, options);

    if (!response.ok) {
      return res.status(400).send("Error while retrieving AQI data");
    }

    const aqiData = await response.json();
    
    return res.status(200).send(aqiData);
  } catch (err) {
    console.log("Error:", err);
  }
});

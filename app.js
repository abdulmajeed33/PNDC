const express = require("express");
const axios = require("axios");

const app = express();

// Region-specific worker nodes
const WORKER_NODES = {
  europe: ["http://localhost:3001"],
  asia: ["http://localhost:3002"],
  gulf: ["http://localhost:3003"],
};

let workerStatus = {}; // Tracks the status of each worker node
const CHECK_INTERVAL = 5000; // Check worker status every 5 seconds
const TIMEOUT = 2000; // Timeout for worker responses in milliseconds
const MAX_RETRIES = 3; // Max retries for querying workers
const FALLBACK_REGIONS = {
  europe: ["asia", "gulf"],
  asia: ["gulf", "europe"],
  gulf: ["europe", "asia"],
};

// Initialize worker status for each region
for (const region in WORKER_NODES) {
  workerStatus[region] = WORKER_NODES[region].reduce((acc, worker) => {
    acc[worker] = { available: true, lastChecked: null };
    return acc;
  }, {});
}

console.log("workerStatus", workerStatus);

// Heartbeat check to monitor worker nodes
const checkWorkerStatus = async () => {
  for (const region in WORKER_NODES) {
    for (const worker of WORKER_NODES[region]) {
      try {
        const response = await axios.get(`${worker}/health`, {
          timeout: TIMEOUT,
        });
        if (response.status === 200) {
          // Check if the worker was previously unavailable
          if (!workerStatus[region][worker].available) {
            console.log(
              `Worker ${worker} in region ${region} is back online at ${new Date().toISOString()}`
            );
          }
          workerStatus[region][worker].available = true;
          workerStatus[region][worker].lastChecked = new Date();
        }
      } catch (error) {
        // Log only the first time a worker goes down
        if (workerStatus[region][worker].available) {
          console.error(
            `Worker ${worker} in region ${region} is unavailable: ${error.message}`
          );
        }
        workerStatus[region][worker].available = false;
      }
    }
  }
};

// Start periodic heartbeat checks
setInterval(checkWorkerStatus, CHECK_INTERVAL);

// Function to query a worker with retries
const queryWorkerWithRetries = async (worker, query, retries = MAX_RETRIES) => {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await axios.get(
        `${worker}/search?q=${encodeURIComponent(query)}`,
        { timeout: TIMEOUT }
      );
      return response.data.results;
    } catch (error) {
      console.error(
        `Attempt ${i + 1} failed for worker ${worker}: ${error.message}`
      );
    }
  }
  workerStatus[worker].available = false; // Mark worker as unavailable after retries
  return [];
};

// Function to query all workers in a region
const queryRegion = async (region, query) => {
  const workers = WORKER_NODES[region] || [];
  const results = await Promise.all(
    workers.map((worker) => {
      if (workerStatus[region][worker].available) {
        return queryWorkerWithRetries(worker, query);
      } else {
        console.log(
          `Skipping worker ${worker} in region ${region} (unavailable).`
        );
        return [];
      }
    })
  );
  return [...new Set(results.flat())]; // Deduplicate results
};

// Endpoint to handle search requests
app.get("/search", async (req, res) => {
  const query = req.query.q;
  const region = req.query.region;

  if (!query || !region) {
    return res
      .status(400)
      .json({ error: "Query parameters 'q' and 'region' are required." });
  }

  if (!WORKER_NODES[region]) {
    return res
      .status(400)
      .json({ error: `Region '${region}' is not recognized.` });
  }

  try {
    let results = await queryRegion(region, query);
    let fallbackRegions = FALLBACK_REGIONS[region] || [];
    let fallbackUsed = null;

    for (const fallbackRegion of fallbackRegions) {
      if (results.length === 0) {
        console.log(`Falling back to region ${fallbackRegion}`);
        results = await queryRegion(fallbackRegion, query);
        if (results.length > 0) {
          fallbackUsed = fallbackRegion;
          break;
        }
      }
    }

    if (results.length === 0) {
      return res.status(200).json({
        query,
        region,
        results,
        message: `cannot find any results in region '${region}' or fallback regions '${fallbackRegions.join(
          ", "
        )}'.`,
      });
    }

    res.json({
      query,
      region: fallbackUsed ? fallbackUsed : region,
      results,
      message: fallbackUsed
        ? `Results fetched from fallback region '${fallbackUsed}' because region '${region}' does not have a result.`
        : `Results fetched from region '${region}'.`,
    });
  } catch (error) {
    console.error(
      `Error fetching results from region '${region}':`,
      error.message
    );
    res
      .status(500)
      .json({ error: `Error fetching results from region '${region}'.` });
  }
});

// Start the central server
const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Central server running on http://localhost:${PORT}`);
});

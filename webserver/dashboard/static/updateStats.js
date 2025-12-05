// URLs containing docker container names will be handled in the nginx config file (turn these paths into IP addresses and ports)
const PROCESSING_STATS_API_URL = "/api/processing/stats" // Stats endpoint for processing (port 8100)
const ANALYZER_API_URL = {
    stats: "/api/analyzer/stats", // Stats endpoint for analyzer (port 8110)
    volume: "/api/analyzer/hair/volume", // Get volume reading at some index endpoint from analyzer
    type: "/api/analyzer/hair/type" // Get type reading at some index endpoint from analyzer
}
const HEALTH_API_URL = "/api/health/statuses"

// This function fetches and updates the general statistics
const makeReq = (url, cb) => {
    fetch(url)
        .then(res => res.json())
        .then((result) => {
            console.log("Received data: ", result)
            cb(result);
        }).catch((error) => {
            updateErrorMessages(error.message)
        })
}

const updateCodeDiv = (result, elemId) => document.getElementById(elemId).innerText = JSON.stringify(result)

const getLocaleDateStr = () => (new Date()).toLocaleString()

const getStats = () => {
    document.getElementById("last-updated-value").innerText = getLocaleDateStr()

    // Get health stats
    makeReq(HEALTH_API_URL, (result) => {
        updateCodeDiv(result.analyzer, "analyzer-status")
        updateCodeDiv(result.processing, "processing-status")
        updateCodeDiv(result.receiver, "receiver-status")
        updateCodeDiv(result.storage, "storage-status")
        updateCodeDiv(result.last_update, "last-update")
    })
    
    makeReq(PROCESSING_STATS_API_URL, (result) => updateCodeDiv(result, "processing-stats"))

    makeReq(ANALYZER_API_URL.stats, (result) => {
        updateCodeDiv(result, "analyzer-stats")
        let num_volume_readings = result.num_volume_readings // Get number of volume readings from stats
        let num_type_readings = result.num_type_readings // Get number of type readings from stats

        // Create random indexes (0 to total readings num - 1) for get requests
        makeReq(`${ANALYZER_API_URL.volume}?index=${Math.floor(Math.random() * Math.abs(num_volume_readings))}`, (result) => {
            updateCodeDiv(result, "event-volume")
        })

        makeReq(`${ANALYZER_API_URL.type}?index=${Math.floor(Math.random() * Math.abs(num_type_readings))}`, (result) => {
            updateCodeDiv(result, "event-type")
        })
    })
}

const updateErrorMessages = (message) => {
    const id = Date.now()
    console.log("Creation", id)
    msg = document.createElement("div")
    msg.id = `error-${id}`
    msg.innerHTML = `<p>Something happened at ${getLocaleDateStr()}!</p><code>${message}</code>`
    document.getElementById("messages").style.display = "block"
    document.getElementById("messages").prepend(msg)
    setTimeout(() => {
        const elem = document.getElementById(`error-${id}`)
        if (elem) { elem.remove() }
    }, 7000)
}

const setup = () => {
    getStats()
    setInterval(getStats(), 4000) // Update every 4 seconds
}

document.addEventListener('DOMContentLoaded', setup)
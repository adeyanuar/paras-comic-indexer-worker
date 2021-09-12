let lastUpdate = {}

const setLastUpdate = (key, value) => {
	lastUpdate[key] = value;
}

const getLastUpdate = () => {
    return lastUpdate;
};

module.exports = {
	setLastUpdate,
    getLastUpdate,
}
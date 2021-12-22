const net = require('net');
const fs = require('fs');

let id;
let port;
let localString;

let otherClients = [];
let myActions = [];
let myTimestamp = 0;
let goodbyeCounter = 0;

let operationsHistory = [];

const divideStringOn = (s, search) => {
    const index = s.indexOf(search)
    if (index < 0) {
        return [s, ''];
    }
    const first = s.slice(0, index)
    const rest = s.slice(index + search.length)
    return [first, rest]
}

// Read the file and create client.
let filename = process.argv[2];
let data = fs.readFileSync(filename, { encoding: 'utf8' });

let client;
let others;
let actions;
[client, others, actions] = data.split('\r\n\r\n', 3)
let rest;
[id, rest] = divideStringOn(client, '\r\n');
[port, localString] = divideStringOn(rest, '\r\n');
while (others !== '') {
    let otherClient;
    [otherClient, others] = divideStringOn(others, '\r\n');
    const [clientId, clientIP, clientPort] = otherClient.split(' ', 3);
    otherClients.push({ clientId, clientIP, clientPort });
}
goodbyeCounter = otherClients.length + 1;
while (actions !== '') {
    let action;
    [action, actions] = divideStringOn(actions, '\r\n');
    let [type, args] = divideStringOn(action, ' ');
    args = args.split(' ');
    switch (type) {
        case 'insert':
            args.length > 1 ?
                myActions.push({ type, character: args[0], index: parseInt(args[1]) }) :
                myActions.push({ type, character: args[0] });
            break;
        case 'delete':
            myActions.push({ type, index: parseInt(args[0]) });
            break;
        default:
            break;
    }
}

const updateTimestamp = (messageTimestamp, eventType) => {
    if (eventType === 'receive') {
        myTimestamp = Math.max(messageTimestamp, myTimestamp);
    }

    myTimestamp++;

    if (eventType === 'send') {
        messageTimestamp = myTimestamp;
    }
}

const updateOperationsHistory = (action, updatedString, timestamp) => {
    operationsHistory.push({ action, updatedString, timestamp });
}

const updateGoodbyeCounter = () => {
    goodbyeCounter--;
    if (goodbyeCounter === 0) {
        console.log(localString);
        clientsSockets.forEach(clientSocket => clientSocket.end())
    }
}

const applyMerge = (action, timestamp, cid) => {
    let index = operationsHistory.length - 1;
    while (operationsHistory[index].timestamp > timestamp) {
        index--;
    }
    if (operationsHistory[index].timestamp === timestamp) {
        while (operationsHistory[index].cid > cid) {
            index--;
        }
    }
    localString = operationsHistory[index].updatedString;
    operationsHistory.splice(index, 0, { action, timestamp, cid });

    for (let i = index + 1; i < operationsHistory.length; i++) {
        applyActionToString(operationsHistory[i].action);
    }
}

function getPosition(string, subString, index) {
    return string.split(subString, index).join(subString).length;
}

const handleData = (socket, buffer) => {
    if (buffer.toString().substring(0, 7) === "goodbye") {
        updateGoodbyeCounter();
    }
    else {
        const parsedMessage = JSON.parse(buffer.toString().substring(0, getPosition(buffer.toString(), "}", 2) + 1));
        const { action, timestamp, cid } = parsedMessage;
        updateTimestamp(timestamp, 'receive');
        console.log('receive', timestamp, 'id', cid);

        const index = operationsHistory.length - 1;
        if (index >= 0 && 
            (timestamp < operationsHistory[index].timestamp || 
                (timestamp === operationsHistory[index].timestamp && id < operationsHistory[index].cid))) {
            applyMerge(action, timestamp, cid);
        }
        else {
            applyActionToString(action);
            updateOperationsHistory(action, localString, timestamp);
        }
    }
}

const applyActionToString = (action) => {
    switch (action.type) {
        case 'insert':
            localString = action.index ? localString.slice(0, action.index).concat(action.character).concat(localString.slice(action.index))
                : localString.concat(action.character);
            break;
        case 'delete':
            localString = localString.slice(0, action.index).concat(localString.slice(action.index + 1));
            break;
        default:
            break;
    }
}

let serversSockets = [];
let clientsSockets = [];

net.createServer()
    .listen(port, "127.0.0.1", () => {
        console.log(`Server listening on 127.0.0.1:${port}`);
    })
    .on('connection', socket => {
        console.log(`Someone connected to your port!`);
        clientsSockets.push(socket);

        socket.on('data', buffer => {
            handleData(socket, buffer);
        })
    });
console.log('Server starting...');

const connectToServers = () => {
    for (const otherClient of otherClients) {
        if (otherClient.clientId > id) {
            let socket = net.createConnection(otherClient.clientPort, otherClient.clientIP, () => {
                console.log(`Client ${id} connected to Client ${otherClient.clientId}`);
            });
            socket.on('data', buffer => {
                handleData(socket, buffer);
            })
            serversSockets.push(socket); // TODO:: check if redundent?
        }
    }
}

connectToServers();

setTimeout(() => {
    eventLoop()
}, 10000)

const eventLoop = async () => {
    const sendMessage = async (action) => {
        applyActionToString(action);
        updateTimestamp(myTimestamp, 'send');
        updateOperationsHistory(action, localString, myTimestamp);
        console.log('send', myTimestamp)

        let buf = { action, timestamp: myTimestamp, cid: id }
        serversSockets.forEach(serverSocket => serverSocket.write(JSON.stringify(buf)));
        clientsSockets.forEach(clientSocket => clientSocket.write(JSON.stringify(buf)));
    }

    for (const action of myActions) {
        setTimeout(() => sendMessage(action), Math.random() * 6000);
    }

}

setTimeout(() => {
    updateGoodbyeCounter();
    serversSockets.forEach(serverSocket => serverSocket.write(`goodbye ${id}`));
    clientsSockets.forEach(clientSocket => clientSocket.write(`goodbye ${id}`));
}, 20000);

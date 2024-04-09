import {promises as fs} from 'node:fs';

const fileName = process.argv[2];


const aggregations = new Map();

const MIN = 0;
const MAX = 1;
const SUM = 2;
const COUNT = 3;


const update = function(name,val){
  const existing = aggregations.get(name);

  if (existing) {
    existing[MIN] = Math.min(existing[MIN], val);
    existing[MAX] = Math.max(existing[MAX], val);
    existing[SUM] += val;
    existing[COUNT]++;
  } else {
    aggregations.set(name, [
      /*min:  */ val,
      /*max:  */ val,
      /*sum:  */ val,
      /*count:*/ 1,
    ]);
  }
}



const STATE_NEWLINE = 0;
const STATE_NAME=1;
const STATE_SEMI=2;
const STATE_NUMBER=3;

const BUFFER_SIZE = 1024;

let buffer = Buffer.allocUnsafe(BUFFER_SIZE);
let backBuffer = Buffer.allocUnsafe(BUFFER_SIZE);

let tempBuffer = Buffer.allocUnsafe(200);

const handle = await fs.open(fileName,"r");
const stat = await handle.stat({});
const size = stat.size;
let state = STATE_NEWLINE;
let nameStart = 0;
let number = 0;
let sign = 1;
let name;
for(let offset = 0; offset < size; offset += BUFFER_SIZE){
  let readSize = BUFFER_SIZE;
  if(offset+BUFFER_SIZE >= size)readSize = size-offset;
  await handle.read(buffer,0,readSize,offset);
  let i = 0;

  // // process the remains from the previous buffer
  if(state === STATE_NAME){
    backBuffer.copy(tempBuffer,0,nameStart,BUFFER_SIZE);
    let nameLength = BUFFER_SIZE-nameStart;
    // reading the rest of the name from the buffer
    for(;i < readSize; i++){
      let c = buffer[i];
      tempBuffer[nameLength++] = c;
      if(c === 0x3b){// ';'
        nameLength--;
        break;
      }
    }

    state = STATE_SEMI;
    // i now points to ';'
    if((nameLength&1) === 1){
      name = tempBuffer.toString("utf16le",0,nameLength+1);
    }else{
      name = tempBuffer.toString("utf16le",0,nameLength);
    }
    i++;
  }
  if(state === STATE_SEMI || state === STATE_NUMBER){
    let c = buffer[i];
    if(c === 0x2d){
      sign = -1;
      i++;
    }
    state = STATE_NUMBER;
    for(;i < readSize; i++){
      let c = buffer[i];
      if(c === 0x2e){// '.'
        continue;
      }else if(c === 0x0a){// '\n'
        break;
      }else{
        number = number*10+(c-0x30);
      }
    }
    i++;

    state = STATE_NEWLINE;
    number = number*sign;
    update(name,number);

    number = 0;
    sign = 1;
  }

  // main loop
  outer:
  while(true){
    state = STATE_NAME;
    nameStart = i;
    for(;i < readSize; i++){
      let c = buffer[i];
      if(c === 0x3b){// ';'
        break;
      }
    }
    if(i === readSize)break outer;


    state = STATE_SEMI;
    // i now points to ';'
    const nameLength = i-nameStart;
    if((nameLength&1) === 1){
      name = buffer.toString("utf16le",nameStart,i+1);
    }else{
      name = buffer.toString("utf16le",nameStart,i);
    }
    i++;
    if(i === readSize)break outer;

    state = STATE_NUMBER;
    // i now points to the first letter in the number section
    let c = buffer[i];
    if(c === 0x2d){
      sign = -1;
      i++;
    }
    if(i === readSize)break outer;

    for(;i < readSize; i++){
      let c = buffer[i];
      if(c === 0x2e){// '.'
        continue;
      }else if(c === 0x0a){// '\n'
        break;
      }else{
        number = number*10+(c-0x30);
      }
    }
    if(i === readSize)break outer;
    // i now points to the new line
    i++;

    state = STATE_NEWLINE;
    number = number*sign;
    update(name,number);

    number = 0;
    sign = 1;
  }
  [buffer,backBuffer] = [backBuffer,buffer];
}
//console.log(aggregations);

printCompiledResults(aggregations);

/**
 * @param {Map} aggregations
 *
 * @returns {void}
 */
function printCompiledResults(aggregations) {
  const sortedStations = Array.from(aggregations.keys()).sort();
  let result = "{" +
  [...aggregations].map(e=>{
    const nameByteLength = e[0].length*2;
    let u8str = Buffer.from(e[0],"utf16le").toString("utf8")//tempBuffer.fill(e[0],0,nameByteLength,"utf16le").toString("utf8",0,e[0].length);
    if(u8str.at(-1) === ";"){
      u8str = u8str.slice(0,-1);
    }
    e[0] = u8str;
    return e;
  }).sort((a,b)=>a[0] < b[0] ? -1 : 1).map(([label,[min,max,sum,count]])=>{
    return `${label}=${round(min / 10)}/${round(
      sum / 10 / count
    )}/${round(max / 10)}`;
  }).join(", ") + "}";
  console.log(result);
}

/**
 * @example
 * round(1.2345) // "1.2"
 * round(1.55) // "1.6"
 * round(1) // "1.0"
 *
 * @param {number} num
 *
 * @returns {string}
 */
function round(num) {
  const fixed = Math.round(10 * num) / 10;

  return fixed.toFixed(1);
}

handle.close();

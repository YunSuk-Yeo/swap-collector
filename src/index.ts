require('dotenv').config();

import { LCDClient, TxInfo } from '@terra-money/terra.js';
import Bluebird from 'bluebird';

const RESULT_FILE_NAME = process.env.RESULT_FILE_NAME as string;
const START_HEIGHT = parseInt(process.env.START_HEIGHT as string);
const END_HEIGHT = parseInt(process.env.END_HEIGHT as string);

const TERRA_URL = process.env.TERRA_URL as string;
const TERRA_CHAIN_ID = process.env.TERRA_CHAIN_ID as string;
const TERRA_TXS_LOAD_UNIT = parseInt(process.env.TERRA_TXS_LOAD_UNIT as string);

const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const csvWriter = createCsvWriter({
  path: RESULT_FILE_NAME,
  header: [
    { id: 'height', title: 'HEIGHT' },
    { id: 'tx_hash', title: 'TX_HASH' },
    { id: 'sender', title: 'SENDER' },
    { id: 'receiver', title: 'RECEIVER' },
    { id: 'offer_amount', title: 'OFFER_AMOUNT' },
    { id: 'offer_denom', title: 'OFFER_DENOM' },
    { id: 'ask_amount', title: 'ASK_AMOUNT' },
    { id: 'ask_denom', title: 'ASK_DENOM' }
  ]
});

const lcdClient = new LCDClient({
  URL: TERRA_URL,
  chainID: TERRA_CHAIN_ID
});

async function main() {
  for (let height = START_HEIGHT; height <= END_HEIGHT; height++) {
    const swapDatas = await load(height);
    if (swapDatas.length > 0) await csvWriter.writeRecords(swapDatas);

    if (height % 100 === 0) {
      console.info(`HEIGHT: ${height}`);
    }

    await Bluebird.delay(10);
  }
}

async function load(
  height: number,
): Promise<SwapData[]> {
  const limit = TERRA_TXS_LOAD_UNIT;
  const swapDatas: SwapData[] = [];

  let page = 1;
  let totalPage = 1;
  do {
    const txResult = await lcdClient.tx.search({
      'tx.height': height,
      page,
      limit
    });

    swapDatas.push(...txResult.txs.map(parseTx).flat());

    totalPage = txResult.page_total;
  } while (page++ < totalPage);

  return swapDatas;
}

function parseTx(txInfo: TxInfo): SwapData[] {
  const swapDatas: SwapData[] = [];

  // Skip when tx is failed
  if (txInfo.code !== undefined) {
    return swapDatas;
  }

  txInfo.tx.msg.forEach((msg, idx) => {
    if (!txInfo.logs) return;

    const data = msg.toData();
    if (data.type === 'market/MsgSwap') {
      swapDatas.push({
        height: txInfo.height,
        tx_hash: txInfo.txhash,
        sender: data.value.trader,
        receiver: data.value.trader,
        offer_denom: data.value.offer_coin.denom,
        offer_amount: data.value.offer_coin.amount,
        ask_denom: data.value.ask_denom,
        ask_amount: txInfo.logs[idx].eventsByType['swap'][
          'swap_coin'
        ][0].replace(/[ a-z]/g, '')
      });
    }

    if (data.type === 'market/MsgSwapSend') {
      swapDatas.push({
        height: txInfo.height,
        tx_hash: txInfo.txhash,
        sender: data.value.from_address,
        receiver: data.value.to_address,
        offer_denom: data.value.offer_coin.denom,
        offer_amount: data.value.offer_coin.amount,
        ask_denom: data.value.ask_denom,
        ask_amount: txInfo.logs[idx].eventsByType['swap'][
          'swap_coin'
        ][0].replace(/[ a-z]/g, '')
      });
    }
  });

  return swapDatas;
}

export type SwapData = {
  height: number;
  tx_hash: string;
  sender: string;
  receiver: string;

  offer_amount: string;
  offer_denom: string;
  ask_amount: string;
  ask_denom: string;
};

main().then(() => {
  console.info('FINISHED');
});

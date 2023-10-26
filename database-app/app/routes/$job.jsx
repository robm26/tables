import {
    useLoaderData, Link, Form, useFetcher, useActionData
}  from "@remix-run/react";

import { json, redirect } from "@remix-run/node";

import React from 'react';

import { Menu } from "~/components/menu";

import {SqlGrid} from "~/components/SqlGrid";

import {runSql} from "~/components/database";


export async function action({ params, request }) {
    const body = await request.formData();
    const limit = (body.get('limit'));

    // const sqlStmt = body?._fields['sqlText'][0];
    const sqlStmt = 'select * from orders limit ' + limit;

    const result = await runSql(sqlStmt);
    // console.log({result:result});
    // const result = [{'name':'Luna'}];

    return({result: result.result, latency: result.latency});
}

export const loader = async ({ params, request }) => {
    return {
        params: params
    };
};


export default function JobIndex() {
    const data = useLoaderData();
    const actionData = useActionData();

   //  const keepfor = 365;
   //  const ttl = '  unix_timestamp(date_add(now(),interval ' + keepfor + ' day)) as ttl\n';


    let stmt = 'hello';

    const [sql, setSql] = React.useState(stmt);

    const clearSql = () => {
        setSql('');
        return null;
    };

    const handleSqlUpdate = (val) => {
        setSql(val.target.value);
    };

    let rows = 0;
    let cols = 0;
    if(actionData?.result && !actionData?.result?.error) {
        let dataset = actionData?.result;
        rows = dataset.length;
        cols = Object.keys(dataset[0]).length;
        // console.log(JSON.stringify(dataset, null, 2));
    }

    const sqlForm = (<Form id="jobForm" method="post"  >
        <table className='sqlTableForm'>
            <thead></thead>
            <tbody><tr><td>

            <label>Limit: <input name="limit" className="limit" type="text" defaultValue="3"/></label>
                <br/>
                <br/>
                <button type='submit'>
                    RUN SQL
                </button>
                &nbsp;&nbsp;&nbsp;
                <Link to={'.'} >
                    <button type='submit' onClick={()=>{ clearSql() }} >CLEAR</button>
                </Link>
                {/*&nbsp; &nbsp; &nbsp;*/}
                {/*{rows && rows > 0 ? (<span>rows: {rows}</span>) : null}*/}
                {/*&nbsp;&nbsp;*/}
                {/*{cols && cols > 0 ? (<span>columns: {cols}</span>) : null}*/}
                <br/>
                {actionData?.latency ? actionData?.latency + ' ms' : ''}
            </td></tr></tbody>
        </table>
    </Form>);

   //  let connLabel = {database:data.database, host:data.host};

    return (
        <div className="rootContainer">
            <Menu  />

            <div className='sqlContainer'>
                {sqlForm}
                <br/>
                {actionData?.result?.error ?
                    (<div className='errorPanel'>{actionData.result.error?.code}<br/>{actionData.result.error?.sqlMessage}</div>)
                    : ( <SqlGrid data={actionData?.result} />)
                }

            </div>

        </div>
    );
}

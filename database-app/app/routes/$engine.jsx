import {
    useLoaderData
} from "@remix-run/react";

import { Menu } from "~/components/menu";


// export async function action({ request }) {
//
//     const body = await request.formData();
//
//     const sqlStmt = body?._fields['sqlText'][0];
//
//     const result = await runSql(sqlStmt);
//     // console.log({result:result});
//
//     return({result: result});
// }

export const loader = async ({ params, request }) => {
    return {
        engine:params.engine
    };
};


export default function TableIndex(params) {

    const data = useLoaderData();
    // const actionData = useActionData();

    return(<div className="rootContainer">
        <Menu choice={data.engine} />
        <br/>
         Choose table
    </div>);
}
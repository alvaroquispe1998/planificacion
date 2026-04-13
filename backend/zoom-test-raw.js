const mysql = require('mysql2/promise');

async function testZoom() {
    const connection = await mysql.createConnection({
        host: '127.0.0.1',
        port: 3307,
        user: 'root',
        password: 'root',
        database: 'uai_planning'
    });

    const [rows] = await connection.execute('SELECT * FROM planning_zoom_config LIMIT 1');
    if (rows.length === 0) {
        console.error('No zoom config found in DB');
        return;
    }
    const config = rows[0];
    
    // get token
    const authorization = Buffer.from(`${config.client_id}:${config.client_secret}`).toString('base64');
    const tokenUrl = `https://zoom.us/oauth/token?grant_type=account_credentials&account_id=${encodeURIComponent(config.account_id)}`;
    
    console.log('Fetching Zoom token...');
    const tokenRes = await fetch(tokenUrl, {
        method: 'POST',
        headers: {
            authorization: `Basic ${authorization}`,
            'content-type': 'application/x-www-form-urlencoded',
        }
    });
    const tokenData = await tokenRes.json();
    if (!tokenData.access_token) {
        console.error('No token', tokenData);
        process.exit(1);
    }
    
    const token = tokenData.access_token;
    console.log('Fetching basic meeting info (GET /users/id/meetings)...');
    
    const userRes = await fetch(`https://api.zoom.us/v2/users/uai0001@autonomadeica.edu.pe/meetings?page_size=30`, {
        headers: { authorization: `Bearer ${token}` }
    });
    const userData = await userRes.json();
    console.log('Matched meeting from list array view:');
    const meetingListItem = (userData.meetings || []).find(m => m.id.toString() === '89795676909');
    console.log(meetingListItem || 'Not found in list');

    console.log('\nFetching meeting detail (GET /meetings/89795676909)...');
    const meetingRes = await fetch(`https://api.zoom.us/v2/meetings/89795676909`, {
        headers: { authorization: `Bearer ${token}` }
    });
    const meetingData = await meetingRes.json();
    
    console.log('\nStart URL in detail:', meetingData.start_url || 'MISSING');
    console.log('Join URL in detail:', meetingData.join_url);
    console.log('Full detail payload keys:', Object.keys(meetingData));
    
    process.exit(0);
}

testZoom().catch(console.error);

const mysql = require('mysql2/promise');
async function check() {
  try {
    const connection = await mysql.createConnection({
      host: 'localhost',
      port: 3307,
      user: 'root',
      password: 'root',
      database: 'uai_planning'
    });
    const sid = '08de88bd-371a-477c-8d65-3013da3a424e';
    const [rows] = await connection.execute(
        "SELECT s.id as schedule_id, s.day_of_week, s.start_time, sub.id as subsection_id " +
        "FROM planning_subsection_schedules s " +
        "INNER JOIN planning_subsections sub ON sub.id = s.planning_subsection_id " +
        "WHERE sub.planning_section_id = ?", 
        [sid]
    );
    console.log('--- RESULTADOS ---');
    console.log(JSON.stringify(rows, null, 2));
    await connection.end();
  } catch (err) {
    console.error('Error:', err);
  }
}
check();

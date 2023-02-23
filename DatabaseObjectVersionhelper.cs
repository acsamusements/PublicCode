public class DatabaseObjectVersionHelper
    {
        private Version Version { get; set; }
        private String ConnectionString { get; set; }
        private SqlConnection Connection { get; set; }
        private String Schema { get; set; }
        public DatabaseObjectVersionHelper(Version _Version, string _ConnectionString,string _Schema = "dbo")
        {
            Version = _Version;
            ConnectionString = _ConnectionString;
            Connection = new SqlConnection(ConnectionString);
            Schema = _Schema;
        }
        public Int32 SchemaId()
        {
            int result;
            using (Connection)
            {
                Connection.Open();
                SqlCommand cmd = new SqlCommand($"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE [Name]='{Schema}') BEGIN EXEC('CREATE SCHEMA '{Schema}') END; SELECT SCHEMA_ID('{Schema}')", Connection);
                object obj = cmd.ExecuteScalar();
                if (obj != null)
                {
                    if (Int32.TryParse(obj.ToString(), out result))
                        return result;
                    else
                        result = -1;
                }
                else
                    result = -1;
                Connection.Close();
            }
            return result;
        }
        public Boolean Exists(string Name)
        {
            bool result = false;
            using (Connection)
            {
                Connection.Open();
                SqlCommand cmd = new SqlCommand($"SELECT COUNT(*) FROM sys.objects WHERE [Schema_Id]={SchemaId()} AND UPPER([Name])='{Name.ToUpper()}'", Connection);
                object obj = cmd.ExecuteScalar();
                int count;
                if (obj != null)
                    if (Int32.TryParse(obj.ToString(), out count))
                        if (count > 0)
                            result = true;
                Connection.Close();
            }
            return result;
        }
        public Boolean DeleteVersion(string Name, Level1Type level1Type)
        {
            bool result = false;
            using (Connection)
            {
                Connection.Open();
                SqlCommand cmd = new SqlCommand("sp_dropextendedproperty", Connection);
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@name", "VERSION");
                cmd.Parameters.AddWithValue("@level0type", Level0Type.SCHEMA);
                cmd.Parameters.AddWithValue("@level0name", Schema);
                cmd.Parameters.AddWithValue("@level1type", level1Type);
                cmd.Parameters.AddWithValue("@level1name", Name);
                object obj = cmd.ExecuteScalar();
                if (obj != null)
                    if (!string.IsNullOrEmpty(obj.ToString()))
                        if (Int32.TryParse(obj.ToString(), out int count))
                            if (count > 0)
                                result = true;
                Connection.Close();
            }
            return result;
        }
        public Boolean PutVersion(string name, Level1Type level1Type, Version value)
        {
            bool result = false;
            using (Connection)
            {
                Connection.Open();
                SqlCommand cmd = new SqlCommand("sp_addextendedproperty", Connection);
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@name", "VERSION");
                cmd.Parameters.AddWithValue("@value", value.ToString());
                cmd.Parameters.AddWithValue("@level0type", Level0Type.SCHEMA);
                cmd.Parameters.AddWithValue("@level0name", Schema);
                cmd.Parameters.AddWithValue("@level1type", level1Type);
                cmd.Parameters.AddWithValue("@level1name", name);
                object obj = cmd.ExecuteScalar();
                if (obj != null)
                    if (!string.IsNullOrEmpty(obj.ToString()))
                        if (Int32.TryParse(obj.ToString(), out int count))
                            if (count > 0)
                                result = true;
                Connection.Close();
            }
            return result;
        }
        public Boolean SetVersion(string name, Level1Type level1Type, Version value)
        {
            bool result = false;
            using (Connection)
            {
                Connection.Open();
                SqlCommand cmd = new SqlCommand("sp_updateextendedproperty", Connection) { CommandType = System.Data.CommandType.StoredProcedure };
                cmd.Parameters.AddWithValue("@name", "VERSION");
                cmd.Parameters.AddWithValue("@value", value.ToString());
                cmd.Parameters.AddWithValue("@level0type", Level0Type.SCHEMA);
                cmd.Parameters.AddWithValue("@level0name", Schema);
                cmd.Parameters.AddWithValue("@level1type", level1Type);
                cmd.Parameters.AddWithValue("@level1name", name);
                object obj = cmd.ExecuteScalar();
                if (obj != null)
                    if (!string.IsNullOrEmpty(obj.ToString()))
                        if (Int32.TryParse(obj.ToString(), out int count))
                            if (count > 0)
                                result = true;
                Connection.Close();
            }
            return result;
        }
        public Version GetVersion(string name, Level1Type level1Type)
        {
            Version result;
            using (Connection)
            {
                Connection.Open();
                SqlCommand cmd = new SqlCommand($"SELECT VALUE FROM sys.extended_properties WHERE [Name]='VERSION' AND [major_id]=OBJECT_ID('{Schema}.{name}','{level1Type}')", Connection);
                object obj = cmd.ExecuteScalar();
                if (obj != null)
                {
                    string v = obj.ToString();
                    result = new Version(v);
                }
                else
                    result = new Version("0.0.0.0");
                Connection.Close();
            }
            return result;
        }
        public Boolean Run(string file)
        {
            bool result = false;
            using (Connection)
            {
                Connection.Open();
                string script = System.IO.File.ReadAllText(file);
                Server server = new Server(new ServerConnection(Connection));
                object obj = server.ConnectionContext.ExecuteScalar(script);
                int count;
                if (obj != null)
                    if (Int32.TryParse(obj.ToString(), out count))
                        if (count > 0)
                            result = true;
                Connection.Close();
            }
            return result;
        }
    }
    public class Level1Type
    {
        private Level1Type(string value) { Value = value; }
        public String Value { get; private set; }
        public static Level1Type AGGEGATE { get { return new Level1Type("AGGEGATE"); } }
        public static Level1Type DEFAULT { get { return new Level1Type("DEFAULT"); } }
        public static Level1Type FUNCTION { get { return new Level1Type("FUNCTION"); } }
        public static Level1Type LOGICALFILENAME { get { return new Level1Type("LOGICAL FILE NAME"); } }
        public static Level1Type PROCEDURE { get { return new Level1Type("PROCEDURE"); } }
        public static Level1Type QUEUE { get { return new Level1Type("QUEUE"); } }
        public static Level1Type RULE { get { return new Level1Type("RULE"); } }
        public static Level1Type SYNONYM { get { return new Level1Type("SYNONYM"); } }
        public static Level1Type TABLE { get { return new Level1Type("TABLE"); } }
        public static Level1Type TABLE_TYPE { get { return new Level1Type("TABLE_TYPE"); } }
        public static Level1Type TYPE { get { return new Level1Type("TYPE"); } }
        public static Level1Type VIEW { get { return new Level1Type("VIEW"); } }
        public static Level1Type XMLSCHEMACOLLECTION { get { return new Level1Type("XML SCHEMA COLLECTION"); } }
        public static Level1Type NULL { get { return new Level1Type("NULL"); } }
        public override string ToString()
        {
            return Value;
        }
    }
    public class Level0Type
    {
        private Level0Type(string value) { Value = value; }
        public string Value { get; private set; }
        public static Level0Type ASSEMBLY { get { return new Level0Type("ASSEMBLY"); } }
        public static Level0Type CONTRACT { get { return new Level0Type("CONTRACT"); } }
        public static Level0Type EVENTNOTIFICATION { get { return new Level0Type("EVENT NOTIFICATION"); } }
        public static Level0Type FILEGROUP { get { return new Level0Type("FILEGROUP"); } }
        public static Level0Type MESSAGETYPE { get { return new Level0Type("MESSAGE TYPE"); } }
        public static Level0Type PARTITIONFUNCTION { get { return new Level0Type("PARTITION FUNCTION"); } }
        public static Level0Type PARTITIONSCHEME { get { return new Level0Type("PARTITION SCHEME"); } }
        public static Level0Type REMOTESERVICEBINDING { get { return new Level0Type("REMOTE SERVICE BINDING"); } }
        public static Level0Type ROUTE { get { return new Level0Type("ROUTE"); } }
        public static Level0Type SCHEMA { get { return new Level0Type("SCHEMA"); } }
        public static Level0Type SERVICE { get { return new Level0Type("SERVICE"); } }
        public static Level0Type TRIGGER { get { return new Level0Type("TRIGGER"); } }
        public static Level0Type PLANGUIDE { get { return new Level0Type("PLAN GUIDE"); } }
        public static Level0Type NULL { get { return new Level0Type("NULL"); } }
        public override string ToString()
        {
            return Value;
        }
    }

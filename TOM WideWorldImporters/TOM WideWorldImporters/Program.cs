using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AnalysisServices;
using Microsoft.AnalysisServices.Tabular;

namespace TOM_WideWorldImporters
{
    class Program
    {
        static void Main(string[] args)
        {
            string ConnectionString = "DataSource=localhost";
            using (Server server = new Server())
            {
                server.Connect(ConnectionString);

                // ALL FOLLOWING CODE GOES HERE
                string newDB = server.Databases.GetNewName("Wide World Importers");
                var blankdatabase = new Database()
                {
                    Name = newDB,
                    ID = newDB,
                    CompatibilityLevel = 1200,
                    StorageEngineUsed = StorageEngineUsed.TabularMetadata,
                };

                blankdatabase.Model = new Model()
                {
                    Name = "Wide World Importers Model",
                    Description = "The Wide World Imports Tabular data model at the 1200 compatibility level."
                };

                blankdatabase.Model.DataSources.Add(new ProviderDataSource()
                {
                    Name = "WideWorldImportersDW_Source",
                    ConnectionString = "Provider=SQLNCLI11;Data Source=localhost;Initial Catalog=WideWorldImportersDW;Integrated Security=SSPI;Persist Security Info=false",
                    ImpersonationMode = Microsoft.AnalysisServices.Tabular.ImpersonationMode.ImpersonateServiceAccount,
                });

                //COLUMN DEFINITIONS
                //DIMENSION TABLES
                //DATE
                DataColumn Date_Date = new DataColumn()
                {
                    Name = "Date",
                    DataType = DataType.DateTime,
                    SourceColumn = "Date",
                    IsUnique = true,
                    FormatString = "yyyy-mm-dd",
                };

                DataColumn Date_MonthNumber = new DataColumn()
                {
                    Name = "Month Number",
                    DataType = DataType.Int64,
                    SourceColumn = "MonthNumber",
                    IsHidden = true,
                };

                DataColumn Date_Month = new DataColumn()
                {
                    Name = "Month",
                    DataType = DataType.String,
                    SourceColumn = "Month",
                    SortByColumn = Date_MonthNumber
                };

                DataColumn Date_Year = new DataColumn()
                {
                    Name = "Year",
                    DataType = DataType.String,
                    SourceColumn = "Year",
                };

                //EMPLOYEE
                DataColumn Employee_EmployeeKey = new DataColumn()
                {
                    Name = "Employee Key",
                    DataType = DataType.Int64,
                    SourceColumn = "EmployeeKey",
                    IsHidden = true,
                    IsUnique = true,
                };

                DataColumn Employee_EmployeeName = new DataColumn()
                {
                    Name = "Employee Name",
                    DataType = DataType.String,
                    SourceColumn = "Employee",
                };

                DataColumn Employee_WWIEmployeeID = new DataColumn()
                {
                    Name = "WWI Employee ID",
                    DataType = DataType.Int64,
                    SourceColumn = "WWIEmployeeID",
                    SummarizeBy = AggregateFunction.None,
                };

                DataColumn Employee_IsSalesPerson = new DataColumn()
                {
                    Name = "Is Sales Person",
                    DataType = DataType.String,
                    SourceColumn = "IsSalesPerson",
                };

                //FACT TABLE
                //ORDER
                DataColumn Order_SalesPersonKey = new DataColumn()
                {
                    Name = "Sales Person Key",
                    DataType = DataType.Int64,
                    SourceColumn = "SalesPersonKey",
                    IsHidden = true,
                };

                DataColumn Order_Date = new DataColumn()
                {
                    Name = "Date",
                    DataType = DataType.DateTime,
                    SourceColumn = "OrderDateKey",
                    IsHidden = true,
                };

                DataColumn Order_TotalExcludingTax = new DataColumn()
                {
                    Name = "TotalExcludingTax",
                    DataType = DataType.Decimal,
                    SourceColumn = "TotalExcludingTax",
                    IsHidden = true,
                };

                DataColumn Order_TaxAmount = new DataColumn()
                {
                    Name = "TaxAmount",
                    DataType = DataType.Decimal,
                    SourceColumn = "TaxAmount",
                    IsHidden = true,
                };

                DataColumn Order_TotalIncludingTax = new DataColumn()
                {
                    Name = "TotalIncludingTax",
                    DataType = DataType.Decimal,
                    SourceColumn = "TotalIncludingTax",
                    IsHidden = true,
                };

                Hierarchy H1 = new Hierarchy()
                {
                    Name = "Calendar Year",
                };

                H1.Levels.Add(new Level()
                {
                    Column = Date_Year,
                    Ordinal = 0,
                    Name = Date_Year.Name
                });

                H1.Levels.Add(new Level()
                {
                    Column = Date_Month,
                    Ordinal = 1,
                    Name = Date_Month.Name
                });

                H1.Levels.Add(new Level()
                {
                    Column = Date_Date,
                    Ordinal = 2,
                    Name = Date_Date.Name
                });

                //TABLES -------------------
                //DATE
                blankdatabase.Model.Tables.Add(new Table()
                {
                    Name = blankdatabase.Model.Tables.GetNewName("Date"),
                    Partitions =
                    {
                        new Partition()
                        {
                            Name = "All Dates",
                            Source = new QueryPartitionSource()
                            {
                                DataSource = blankdatabase.Model.DataSources["WideWorldImportersDW_Source"],
                                Query = @"SELECT Date,Month,[Calendar Month Number] as MonthNumber,[Calendar Year] as Year
                                          FROM Dimension.Date",
                            }
                        }
                    },
                                    Columns =
                    {
                        Date_Date,
                        Date_Year,
                        Date_Month,
                        Date_MonthNumber
                    },
                                    Hierarchies =
                    {
                        H1
                    }
                                });

                                //EMPLOYEE
                                blankdatabase.Model.Tables.Add(new Table()
                                {
                                    Name = blankdatabase.Model.Tables.GetNewName("Employee"),
                                    Partitions =
                    {
                        new Partition()
                        {
                            Name = "All Employees",
                            Source = new QueryPartitionSource()
                            {
                                DataSource = blankdatabase.Model.DataSources["WideWorldImportersDW_Source"],
                                Query = @"SELECT [Employee Key] as EmployeeKey,Employee,[WWI Employee ID] as WWIEmployeeID,CASE [Is Salesperson] WHEN 1 THEN 'Yes' ELSE 'No' end as IsSalesPerson
                                          FROM Dimension.Employee",
                            }
                        }

                    },
                                    Columns =
                    {
                        Employee_EmployeeKey,
                        Employee_EmployeeName,
                        Employee_WWIEmployeeID,
                        Employee_IsSalesPerson
                    }
                                });

                //ORDERS
                blankdatabase.Model.Tables.Add(new Table()
                {
                    Name = blankdatabase.Model.Tables.GetNewName("Orders"),
                    Partitions =
                    {
                        new Partition()
                        {
                            Name = "All Orders",
                            Source = new QueryPartitionSource()
                            {
                                DataSource = blankdatabase.Model.DataSources["WideWorldImportersDW_Source"],
                                Query = @"SELECT [Salesperson Key] as SalesPersonKey,[Order Date Key] as OrderDateKey,[Total Excluding Tax] as TotalExcludingTax
                                          ,[Tax Amount] as TaxAmount,[Total Including Tax] as TotalIncludingTax
                                          FROM Fact.[Order]",
                            }
                        }
                    },
                                    Columns =
                    {
                        Order_SalesPersonKey,
                        Order_Date,
                        Order_TotalExcludingTax,
                        Order_TaxAmount,
                        Order_TotalIncludingTax
                    },
                                    Measures =
                    {
                        new Measure()
                        {
                            Name = "Total Excluding Tax",
                            Expression = "SUM('Orders'[TotalExcludingTax])",
                            FormatString = "#,###.##",
                        },
                        new Measure()
                        {
                            Name = "Tax Amount",
                            Expression = "SUM('Orders'[TaxAmount])",
                            FormatString = "#,###.##",
                            DisplayFolder = "Tax",
                        },
                        new Measure()
                        {
                            Name = "Total Including Tax",
                            Expression = "SUM('Orders'[TotalIncludingTax])",
                            FormatString = "#,###.##",
                        },
                    }
                });

                SingleColumnRelationship relOrderToDate = new SingleColumnRelationship()
                {
                    Name = "Order_Date_Date_Date",
                    ToColumn = Date_Date,
                    FromColumn = Order_Date,
                    FromCardinality = RelationshipEndCardinality.Many,
                    ToCardinality = RelationshipEndCardinality.One
                };

                blankdatabase.Model.Relationships.Add(relOrderToDate);

                SingleColumnRelationship relOrderToEmployee = new SingleColumnRelationship()
                {
                    Name = "Order_EmployeeKey_Employee_EmployeeKey",
                    ToColumn = Employee_EmployeeKey,
                    FromColumn = Order_SalesPersonKey,
                    FromCardinality = RelationshipEndCardinality.Many,
                    ToCardinality = RelationshipEndCardinality.One
                };

                blankdatabase.Model.Relationships.Add(relOrderToEmployee);

                try
                {
                    server.Databases.Add(blankdatabase);
                    blankdatabase.Update(UpdateOptions.ExpandFull);

                    Console.WriteLine("Deployed to server successfully");
                }
                catch
                {
                    Console.WriteLine("Deployed to server failed");
                    return;
                }

                blankdatabase.Model.RequestRefresh(Microsoft.AnalysisServices.Tabular.RefreshType.Full); //request data refresh
                blankdatabase.Update(UpdateOptions.ExpandFull); //execute data refresh

                Console.WriteLine("Data loaded...");

                Console.Write("Database ");
                Console.ForegroundColor = ConsoleColor.Green;
                Console.Write(blankdatabase.Name);
                Console.ResetColor();
                Console.WriteLine(" created successfully.");

                Console.WriteLine("The data model includes the following table definitions:");
                Console.ForegroundColor = ConsoleColor.Yellow;
                foreach (Table tbl in blankdatabase.Model.Tables)
                {
                    Console.WriteLine("\tTable name:\t\t{0}", tbl.Name);
                    //Console.WriteLine("\ttbl description:\t{0}", tbl.Description);

                    foreach (Measure measures in tbl.Measures)
                    {
                        Console.WriteLine("\tMeasure name:\t\t{0}", measures.Name);
                    }

                    foreach (Column columns in tbl.Columns)
                    {
                        Console.WriteLine("\tColumn name:\t\t{0}", columns.Name);
                    }

                    foreach (Hierarchy hierarchy in tbl.Hierarchies)
                    {
                        Console.WriteLine("\tHierachy name:\t\t{0}", hierarchy.Name);
                    }
                }

                foreach (Relationship relationship in blankdatabase.Model.Relationships)
                {
                    Console.WriteLine("\tRelationship name:\t\t{0}", relationship.Name);
                }
                Console.ResetColor();
                Console.WriteLine();

                Console.WriteLine("Press Enter to close this console window.");
                Console.ReadLine();
            }
        }
    }
}

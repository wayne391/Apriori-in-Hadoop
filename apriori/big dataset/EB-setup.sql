remark Jacob Verburg
remark jverburg@calpoly.edu

create table goods ( 
   Id int,
   Flavor varchar2(15), 
   Food varchar2(15), 
   Price float, 
   Type varchar2(5),
   constraint gpid PRIMARY KEY (Id)
);

create table location ( 
   City varchar2(15),
   State varchar2(15), 
   Zip int, 
   Street varchar2(20), 
   StoreNum int, 
   constraint lpid PRIMARY KEY (StoreNum)
);

create table employee ( 
   Last varchar2(15),
   First varchar2(15), 
   HireDate  Date, 
   FireDate  Date, 
   Position varchar2(15), 
   FullTime varchar2(5), 
   StoreNum int, 
   EmpId int, 
   constraint efid FOREIGN KEY (StoreNum) REFERENCES location, 
   constraint epid PRIMARY KEY (EmpId)
);

create table receipts ( 
  ReceiptNumber int,
  SaleDate date,
  Weekend varchar2(5), 
  isCash varchar2(5), 
  EmpId int, 
  StoreNum int, 
  constraint rfid FOREIGN KEY (StoreNum) REFERENCES location, 
  constraint rffid FOREIGN KEY (EmpId) REFERENCES employee, 
  constraint rpid PRIMARY KEY (ReceiptNumber)
);

create table items ( 
   Receipt int,
   Quantity int, 
   Item int, 
   constraint ifid FOREIGN KEY (Receipt) REFERENCES receipts, 
   constraint iffid FOREIGN KEY (Item) REFERENCES goods,
   constraint ipid PRIMARY KEY(Receipt, Item)
 );


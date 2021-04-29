#r "Newtonsoft.Json"

using System.Net;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Primitives;
using System;
using System.IO;
using System.Linq;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Collections.Specialized;
using Savage.Range;
using System.Text.RegularExpressions;

const int digitWidth = 24;
static var columns = new LinkedList<ColumnType>(new List<ColumnType>(){
        new ColumnType {Name = "RNPADRPL", MinLength = 1, MaxLength = 1, Position = new Range<int>(200, 200 + (digitWidth*1))},
        new ColumnType {Name = "RIDADRPL", MinLength = 3, MaxLength = 7, Position = new Range<int>(340, 340 + (digitWidth*7))},
        new ColumnType {Name = "DATUMLOP", MinLength = 17, MaxLength = 17, Position = new Range<int>(570, 570 +  (digitWidth*17))},
        new ColumnType {Name = "LGHNR", MinLength = 4, MaxLength = 4, Position = new Range<int>(1000, 1000 + (digitWidth*4)), Validate = (c, w)=> {return w.Text.All(char.IsDigit) && w.Text.Length == c.MaxLength;}},
        new ColumnType {Name = "LGHSTATUS", MinLength = 1, MaxLength = 1, Position = new Range<int>(1160, 1160 + (digitWidth*1))},
        new ColumnType {Name = "BOAREA", MinLength = 0, MaxLength = 7, Position = new Range<int>(1520, 1520 + (digitWidth*7)), Validate = (c, w) => {double d; return double.TryParse(w.Text, out d) && w.Text.Length <= c.MaxLength && w.Text.Contains(",");}},
        new ColumnType {Name = "ANTRUM", MinLength = 1,  MaxLength = 2, Position = new Range<int>(1850, 1850 + (digitWidth*3)) },
        new ColumnType {Name = "KOKTYP", MinLength = 1,  MaxLength = 1, Position = new Range<int>(2070, 2070 + (digitWidth*2)) },
        new ColumnType {Name = "LGHKAT", MinLength = 1,  MaxLength = 1, Position = new Range<int>(2110, 2110 + (digitWidth*2)) },
        new ColumnType {Name = "NYREGORS", MinLength = 1,  MaxLength = 1, Position = new Range<int>(2530, 2530 + (digitWidth*1))},
        new ColumnType {Name = "FNR", MinLength = 9, MaxLength = 9, Position = new Range<int>(2570, 2570 +(digitWidth*9))},
        new ColumnType {Name = "XKOORD", MinLength = 11, MaxLength = 11, Position = new Range<int>(2840, 2840+(digitWidth*11)), Validate = (col, w) => {return w.Text.All(c => char.IsDigit(c) || c == ',' || c=='.') && w.Text.Length >= col.MinLength && w.Text.Length <= col.MaxLength && (w.Text.Contains(",") || w.Text.Contains("."));}},
        new ColumnType {Name = "YKOORD", MinLength = 10, MaxLength = 10, Position = new Range<int>(3130, 3130+(digitWidth*10)), Validate = (col, w) => {return w.Text.All(c => char.IsDigit(c) || c == ',' || c=='.') && w.Text.Length >= col.MinLength && w.Text.Length <= col.MaxLength && (w.Text.Contains(",") || w.Text.Contains("."));}},
});

private static ILogger logger;
public static async Task<IActionResult> Run(HttpRequest req, ILogger log)
{
    logger = log;
    log.LogInformation("C# HTTP trigger function processed a request.");

    string name = req.Query["name"];

    string text = await new StreamReader(req.Body).ReadToEndAsync();        
    var rows = CorrectFaultyRows(TransformJsonToRows(JObject.Parse(text)));
    var result = TransformRowsTooListOfWordDictionaries(rows);
    return new OkObjectResult(JsonConvert.SerializeObject(result, Formatting.Indented));
}

private static IEnumerable<dynamic> TransformRowsTooListOfWordDictionaries(List<Row> rows){
      return rows.OrderBy(r => r.AverageY)
        .Select(r => 
            new {IsValid = r.IsValid, Words = r.words.OrderBy(w => w.X)
            .GroupBy(w => w.Name, w => w, (key, val) => 
                new {Name = key, Text = val.Select(w => w.Text)
                    .Aggregate((current, next) => $"{current}{next}")})
                        .ToDictionary(w => w.Name, w => w.Text)});
}

private static List<Row> TransformJsonToRows(dynamic json){
    var rows = new List<Row>();
    foreach(dynamic region in json.regions){
        foreach(dynamic line in region.lines){
            foreach(dynamic word in line.words){
                var selectedRows = rows.Where(row => row.FallsWithin(word));
                if(!selectedRows.Any()){
                    rows.Add(new Row(new Word(word)));
                }else{                    
                    selectedRows.First().Add(word);
                }
            }
        }     
    }

    return rows;
}

private static List<Row> CorrectFaultyRows(List<Row> rows){

    // Set column type on unknowns based on postion
    rows.SelectMany(d => d.words).Where(w => w.Name == "Unknown").ToList().ForEach(w => {
        w.BestMatchingColumn = columns.OrderBy(c => Math.Abs(c.Position.Floor - w.X)).FirstOrDefault();
    });

    // Group duplicated words
    var duplicatedEnumerable = rows.SelectMany(r => r.words
        .GroupBy(w => w.Name)
        .Where(grp => grp.Count() > 1)
        .Select(grp => new {Row = r, DuplicatedKey = grp.Key, Words =  grp.Select(w => w)}));    
    
    // Set column on duplicated words based on position
    duplicatedEnumerable.SelectMany(d => d.Words).ToList().ForEach(w => {
        w.BestMatchingColumn = columns.OrderBy(c => Math.Abs(c.Position.Floor - w.X)).FirstOrDefault();
    });
    
    // Replace duplicated words for merged valid ones
    duplicatedEnumerable
        .Where(w => w.Words.Count() == 2)
        .Select(d => new {MergedWord = d.Words.Aggregate((c, n) => c + n), Row = d.Row, Words = d.Words})
        .Where(w => w.MergedWord.IsValid)
        .ToList()
        .ForEach(w => {
            w.Words.ToList().ForEach(wr => w.Row.words.Remove(wr));
            w.Row.words.AddLast(w.MergedWord);            
        });

    // Merge and insert comma on duplicated words that should contain commas
    duplicatedEnumerable
        .Where(dw => dw.DuplicatedKey == "BOAREA" || dw.DuplicatedKey.Contains("KOORD"))
        .Where(dw => dw.Words.Count() == 2)
        .Where(dw => dw.Words.All(w => !w.Text.Contains(",")))
        .Select(dw => new {
                MergedWord = dw.Words.Aggregate((c, n) => {
                    var cc = c.Clone();
                    cc.BestMatchingColumn = c.BestMatchingColumn;
                    cc.Text+=",";                    
                    return cc+n;
                }),
                Row = dw.Row,
                Words = dw.Words})
        .ToList()
        .ForEach(w => {
            logger.LogInformation($"{w.MergedWord.Text}");
            w.Words.ToList().ForEach(wr => w.Row.words.Remove(wr));
            w.Row.words.AddLast(w.MergedWord);            
        });
      
      // Insert commas on cordinates that is still missing
      rows.SelectMany(r => r.words)
        .Where(w => !w.IsValid)
        .Where(w => w.Name.Contains("KOORD"))
        .Where(w => !w.Text.Contains(","))
        .Where(w => w.Text.Length == (w.BestMatchingColumn.MaxLength - 1)).ToList()
        .ForEach(kw => {kw.Text = Regex.Replace(kw.Text, "(\\d{3})$", ",$1");});

    return rows;
}

private class Row {        
   private string[] mandatoryColumns = new []{"RNPADRPL", "RIDADRPL", "DATUMLOP"};
    public LinkedList<Word> words = new LinkedList<Word>();

    public double AverageY => words.Average(w => w.Y);

    public bool IsValid => words.All(w => w.IsValid) && 
        words.Select(w => w.Name).Intersect(mandatoryColumns).Count() == mandatoryColumns.Length;

    public Row(Word word){        
        FixCommonProblems(word).ForEach(w => words.AddLast(w));
    }

    private List<Word> FixCommonProblems(Word unknown){        
        if(unknown.Text.Any(c => c == 'o')){
            unknown.Text = unknown.Text.Replace('o', '0');
        }

        if(unknown.Text.Any(c => c == '.')){
            unknown.Text = unknown.Text.Replace('.', ',');
        }

        if(columns.Where(c => c.Name == "KOKTYP" || c.Name == "LGHKAT").Where(c => c.Position.InRange(unknown.X)).Any(c =>  unknown.Text.Length > c.MaxLength && unknown.Text.Length == 2)){
            var LGHKAT = unknown.Clone();
            LGHKAT.Text = $"{unknown.Text[1]}";                        
            LGHKAT.BestMatchingColumn = columns.First(c => c.Name == "LGHKAT");
            var KOKTYP = unknown.Clone();
            KOKTYP.Text = $"{unknown.Text[0]}";            
            KOKTYP.BestMatchingColumn = columns.First(c => c.Name == "KOKTYP");
            return new List<Word>(){KOKTYP, LGHKAT};
        }
        return new List<Word>(){unknown};
    }
   

    public bool FallsWithin(dynamic w){
        var word = new Word(w);
        var medianHeight = MedianHeight();
        var medianY = MedianY();

        return word.Y < (medianY + medianHeight) && word.Y > (medianY - medianHeight);
    }

    

    private float MedianY(){
        int count = words.Count();
        var horizontalOrderedWords = words.OrderBy(wo => wo.Y);
        float median = horizontalOrderedWords.ElementAt(count/2).Y + horizontalOrderedWords.ElementAt((count-1)/2).Y;
        median /= 2;

        return median;
    }

    private float MedianHeight(){
        int count = words.Count();
        var horizontalOrderedWords = words.OrderBy(wo => wo.Height);
        float median = horizontalOrderedWords.ElementAt(count/2).Height + horizontalOrderedWords.ElementAt((count-1)/2).Height;
        median /= 2;

        return median;
    }

    public void Add(dynamic word){
        FixCommonProblems(new Word(word)).ForEach(w => words.AddLast(w));
    }

    public void Print(){
        words.OrderBy(w => w.X).ToList().ForEach(w => {        
                Console.Write($"|{w.X}:{w.Name}:{w.Text}|");        
        });
        Console.WriteLine();
    }   
}

private class Word {

    public ColumnType BestMatchingColumn;

    public bool IsValid => BestMatchingColumn?.Validate(BestMatchingColumn, this) ?? false;


    public int Y {get;private set;}     
    public int X {get;private set;}
    public int Width {get;private set;}
    public int Height {get;private set;}

    public string Text {get; set;}
    public string Name => BestMatchingColumn?.Name ?? "Unknown";

    public Word(dynamic word){        
        try{
            Text = word.text;
            var values = word.boundingBox.ToString().Split(",");
            X = Int32.Parse(values[0]);
            Y = Int32.Parse(values[1]);
            Width = Int32.Parse(values[2]);
            Height = Int32.Parse(values[3]);
        }catch(Exception e){
            Console.Write($"Error: {e}");            
        }

        SetColumn();
        
    }

    private Word(){    }

    public Word Clone(){
        var w = new Word(){
            Text = this.Text,
            X = this.X,
            Y = this.Y,
            Width = this.Width,
            Height = this.Height            
        };
        w.SetColumn();
        return w;
    }

    public static Word operator +(Word left, Word right){
        return new Word(){
            Text = $"{left.Text}{right.Text}",
            X = Math.Min(left.X, right.X),
            Y = Math.Max(left.Y, right.Y),
            Width = left.Width + right.Width,
            Height = Math.Max(left.Height, right.Height),
            BestMatchingColumn = left.BestMatchingColumn
        };
    }

     public void SetColumn(){
        BestMatchingColumn = FindBestMatchingColumn();
    }

    private ColumnType FindBestMatchingColumn(){

        var cs = columns
            .Where(c => c.Position.InRange(X))//Find columns that match with text X point
            .Where(c => c.Validate(c, this));// And has the corret format            

        if(cs.Any())
            return cs.First();
        
        cs = columns
            .Where(c => c.Validate(c, this))//Find columns that has the correct format
            .OrderBy(c => Math.Abs(c.MaxLength - Text.Length))//Order by the columns that are closest in length
            .ThenBy(c => Math.Abs((c.Position.Floor) - (X)));//Then by the ones that are closest in position

        return cs.FirstOrDefault();
    }

}

private class ColumnType {
    public string Name {get;set;}
    public int MaxLength {get;set;}    
    public int MinLength {get;set;}
    public Range<int> Position {get;set;}
    public Func<ColumnType, Word, bool> Validate {get;set;} = (c, w)=> {    
        return w.Text.All(char.IsDigit) && //Default column must only be digits
            w.Text.Length >= c.MinLength && w.Text.Length <= c.MaxLength;// Default column must fall within boundaries
    };
}

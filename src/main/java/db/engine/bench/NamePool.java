package db.engine.bench;

import java.util.concurrent.ThreadLocalRandom;

public class NamePool {
        
    private static final String[] FIRST = {
        "Alex","Sam","Jordan","Taylor","Chris","Morgan","Casey","Jamie","Avery","Riley",
        "Cameron","Drew","Elliot","Hayden","Jesse","Kai","Logan","Micah","Noel","Quinn",
        "Adrian","Blake","Charlie","Dakota","Emerson","Finley","Grayson","Harper","Indigo","Jules",
        "Keegan","Lane","Marley","Nico","Oakley","Parker","Rowan","Sawyer","Tegan","Urban",
        "Vaughn","Wes","Xander","Yael","Zane","Aria","Briar","Corin","Dane","Eden",
        "Frankie","Galen","Hollis","Idris","Jory","Koda","Larkin","Maddox","Nova","Orrin",
        "Presley","Quest","Reese","Sage","Tristan","Uma","Valen","Winter","Xavi","Yuri","Zuri",
        "Ben","Max","Liam","Owen","Eli","Milo","Hugo","Theo","Levi","Ezra",
        "Iris","Luna","Maya","Nina","Opal","Rhea","Skye","Vera","Wren","Zara"
    };
    private static final String[] LAST = {
            "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez","Martinez",
        "Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor","Moore","Jackson","Martin",
        "Lee","Perez","Thompson","White","Harris","Sanchez","Clark","Ramirez","Lewis","Robinson",
        "Walker","Young","Allen","King","Wright","Scott","Torres","Nguyen","Hill","Flores",
        "Green","Adams","Nelson","Baker","Hall","Rivera","Mitchell","Carter","Roberts","Gomez",
        "Phillips","Evans","Turner","Diaz","Parker","Cruz","Edwards","Collins","Reyes","Stewart",
        "Morris","Morales","Murphy","Cook","Rogers","Gutierrez","Ortiz","Morgan","Cooper","Peterson",
        "Bailey","Reed","Kelly","Howard","Ramos","Kim","Cox","Ward","Richardson","Watson",
        "Brooks","Chavez","Wood","James","Bennett","Gray","Mendoza","Ruiz","Hughes","Price"
    };

    public String randomFullName() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        return FIRST[rnd.nextInt(FIRST.length)] + " " + LAST[rnd.nextInt(LAST.length)];
    }
}
